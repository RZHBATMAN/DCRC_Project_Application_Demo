import os

import pdb

import platform
import math
import json
import textwrap
from datetime import date
from pathlib import Path


import pandas as pd 
import numpy as np 
import pyodbc
from jinja2 import Template

def abspath(path, file = None):
    if file:
        path = path + file
    return f"'{os.path.abspath(path)}'"


class DB:

    # Template sql for updating or inserting data into tables
    upsert_sql = Template(textwrap.dedent("""\
        BEGIN;
        CREATE temp TABLE batch (LIKE {{ tablename }} including ALL) ON COMMIT DROP;
        COPY batch FROM {{ datafile }} WITH csv delimiter ',';
        WITH upd AS (
            UPDATE {{ tablename }} AS tbl
            SET (
                    {{ item_id }}, instrument_id, data_date, data_value,
                    {% if currency %} currency, {% endif %}
                    update_date
                    {% if built_id %} ,{{ built_id }} {% endif %}
                ) = (
                    batch.{{ item_id }}, batch.instrument_id, batch.data_date, batch.data_value,
                    {% if currency %} batch.currency, {% endif %}
                    batch.update_date
                    {% if built_id %} ,batch.{{ built_id }} {% endif %}
                )
            FROM batch
            WHERE ({% if snapshots %}batch.{{ built_id }},{% endif %} 
                   batch.{{ item_id }}, batch.instrument_id, batch.data_date)
                = ({% if snapshots %}tbl.{{ built_id }},{% endif %}
                   tbl.{{ item_id }}, tbl.instrument_id, tbl.data_date)
            AND   batch.data_value <> tbl.data_value
            RETURNING tbl.{{ item_id }}
        ), ins AS (
            INSERT INTO {{ tablename }}
            SELECT *
            FROM batch
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM {{ tablename }}
                    WHERE ({% if snapshots %}batch.{{ built_id }},{% endif %}
                           batch.{{ item_id }}, batch.instrument_id, batch.data_date) 
                        = ({% if snapshots %}{{ tablename }}.{{ built_id }},{% endif %}
                           {{ tablename }}.{{ item_id }},
                           {{ tablename }}.instrument_id,
                           {{ tablename }}.data_date)
                )
            RETURNING {{ tablename }}.{{ item_id }}
        )
        SELECT (
                SELECT COUNT(*)
                FROM upd
            ) AS updates,
            (
                SELECT COUNT(*)
                FROM ins
            ) AS inserts;
        COMMIT;""")   # end of textwrap.dedent
    )    # end of jinja2 Template


    def __init__(self, work_path, user, pwd, driver_path = None):
        # Figure out drive_path if not provided by users
        if not driver_path:   # driver_path is None or empty string
            os_ = platform.system()
            if os_ == 'Darwin':   # Mac OS
                driver_path = '/usr/local/lib/psqlodbcw.so'   # if u install postgresql via brew
            elif os_ == 'Windows':
                driver_path = 'PostgreSQL Unicode(x64)'
            elif os_ == 'Linux':
                driver_path = '/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so'
            else:
                raise ValueError('Unsupported system!')
   
        self.work_path = work_path
        if work_path[-1] != '/':
            self.work_path = work_path + '/'
        
        self.tmp_data_path = os.path.join(self.work_path, 'tmp/')
        Path(self.tmp_data_path).mkdir(parents=True, exist_ok=True)

        self.connect_str = f'DRIVER={{{driver_path}}};SERVERNAME=localhost;PORT=5432;DATABASE=quant_staging;UID={user};PWD={pwd}'


    def __enter__(self):
        self.cnxn = pyodbc.connect(self.connect_str)
        self.cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        self.cnxn.setencoding(encoding='utf-8')
        self.cnxn.maxwrite = 1024 * 1024 * 1024

        # Create a cursor from the connection
        self.cursor = self.cnxn.cursor()
        self.u_date = f'{date.today()}'    # update date

        self.inst_ids = None      # lazy set them up in preload 
        self.inst_exchanges = None
        self.univs = None
        self.items = None         # would be list of tuples of (id, name) for dateitems
        self.factors = None


    def __exit__(self, type, value, traceback):
        self.cnxn.close()


    def _preload(self):
        self.cursor.execute("SELECT id,exchange FROM instruments ORDER BY id")
        self.inst_ids, self.inst_exchanges = zip(*list(self.cursor))

        self.cursor.execute("SELECT id,name FROM data_items")
        self.items = list(self.cursor)

        self.cursor.execute("SELECT id,name FROM indexes")
        self.univs = list(self.cursor)


    def _dump_data(self, datafile, name, df, item_id = None, built_id = None, currency = True, value_type = 0):            
        if item_id is None:
            id = next((i[0] for i in self.items if i[1] == name))

        with open(datafile, 'w') as file:
            for inst_id, val in df.items():  # loop through columns
                #pdb.set_trace()
                try:
                   self.inst_ids.index(inst_id)
                except ValueError:
                    #pdb.set_trace()
                    print(f'{inst_id} not found in instruments table, ignored!!!')
                    continue  # ignore invalid inst_ids
                for d, v in val.items():
                    if not v: 
                        continue
                    if type(v) is float and math.isnan(v):
                        continue

                    # this corresponds to upsert_sql template
                    if built_id is not None:
                        file.write(f"{built_id},")
                    # no use of built_id: currently for rebal_weights which treats built_id as item_id
                    if value_type == 0:
                        file.write(f"{item_id},{inst_id},{d:%Y-%m-%d},{v:.15f},")
                    elif value_type == 1:
                        file.write(f"{item_id},{inst_id},{d:%Y-%m-%d},{v},")
                    elif value_type == 2:
                        file.write(f"{item_id},{inst_id},{d:%Y-%m-%d},{v:%Y-%m-%d},")
                    else:
                        raise ValueError(f'_DB._dumpdata: value_type={value_type} invalid')

                    if currency is True:
                        file.write("'',")
                    file.write(f"{self.u_date}\n")
                        

    def get_exchanges_dict(self, inst_ids):
        if self.inst_ids is None:
            self._preload()

        exchanges_dict={}
        for i in inst_ids:
            pos = self.inst_ids.index(i)  # ValueError throws if can not find which imply logic error!
            exchange = self.inst_exchanges[pos]
            #exchanges_dict[i] = exchange
            if exchange in exchanges_dict:
                exchanges_dict[exchange].append(i)
            else:
                exchanges_dict[exchange] = [i]
        return exchanges_dict
    
    
    def get_exchanges(self, inst_ids):
        if self.inst_ids is None:
            self._preload()

        exchanges = []
        for i in inst_ids:
            pos = self.inst_ids.index(i)  # ValueError throws if can not find which imply logic error!
            exchanges.append(self.inst_exchanges[pos])

        return exchanges
    
    def load_attributes(self,inst_ids,fields,startdate=None,endate=None): #from instruments table
        if self.inst_ids is None:
            self._preload()
        if type(inst_ids) is str:
            inst_ids = [inst_ids]
        if len(inst_ids)==1:
            inst_ids.append('')
        if type(fields) is str:
            fields = [fields]
        sql = f"""SELECT {','.join(fields)} FROM instruments  WHERE  id in {tuple(inst_ids)}"""
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=fields)
        return df 

    def get_insts(self, startdate, enddate, univ_or_insts = None, exchange = None):
        """ Get a list of instrument ids which are occurred during the specified periods

            Parameters:
                startdate, enddate: period to check for instruments
                univ_or_insts: could be 
                a string: representing an index name; or
                a list of strings: representing a list of instrument ids; or
                None: for getting all instrument ids of all indexes
                exchange: a string for a exchange name or None. If provided, instrument ids not
                belong to the exchange will not be included in the returned list.

            Examples:
               inst_ids = db.get_insts('2010-01-01', '2020-01-31', exchange='HK')
        """

        if self.inst_ids is None:
            self._preload()

        inst_ids = None
        if not univ_or_insts:  # univ_or_insts is None or empty ('' or [])
            sql = f"""SELECT DISTINCT instrument_id FROM index_cons
                    WHERE data_date >= '{startdate}' AND data_date <= '{enddate}'"""
            self.cursor.execute(sql)
            inst_ids = [i[0] for i in self.cursor]   # ele of inst_ids is (id,)
        elif type(univ_or_insts) is str:  # univ_or_insts is a universe name
            index_id = self.cursor.execute("SELECT id from indexes where name = ?", univ_or_insts).fetchval()
            sql = f"""SELECT DISTINCT instrument_id FROM index_cons WHERE index_id='{index_id}' 
                      AND data_date >= '{startdate}' AND data_date <= '{enddate}'"""
            self.cursor.execute(sql)
            inst_ids = [i[0] for i in self.cursor]   # ele of inst_ids is (id,)
        elif type(univ_or_insts) is list: # univ_or_insts is a list of inst ids
            inst_ids = univ_or_insts
        
        if not exchange:
            return inst_ids

        insts = []
        for i in inst_ids:
            pos = self.inst_ids.index(i)  # ValueError throws if can not find which imply logic error!
            if self.inst_exchanges[pos] == exchange:
                insts.append(i)
        return insts


    def get_index_info(self, index_name):
        sql = "SELECT * FROM indexes WHERE name=?"
        item = self.cursor.execute(sql, index_name).fetchone()
        return item


    def get_index_id(self, index_name):  # for compatibility
        return self.get_index_info(index_name).id


    def get_item_info(self, name):
        item = self.cursor.execute(f"SELECT * from date_items where name = ?", name).fetchval()
        return item    # id is a named tuple (id, value_type, ...)


    def get_fac_info(self, name):
        item = self.cursor.execute(f"SELECT * from factors where name = ?", name).fetchval()
        return item    # id is a named tuple (id, ...)


    def get_fac_construct_id(self):
        sql = "SELECT max(id) FROM fac_construct"
        id = self.cursor.execute(sql).fetchval()
        return id


    def _tblcases(self, name, item_tbl, snapshots = False):
        """ Internal method used to figure out where to find data for an item given by name from item_tbl """

        value_types = {0: 'raw_data', 1: 'raw_textdata', 2: 'raw_datedata'}  # map from value_type to corresponding raw data table names
        snapshot_map = {
            'raw_data'    : 'raw_snapshots',
            'raw_textdata': 'rawtext_snapshots',
            'raw_datedata': 'rawdate_snapshots',
            'fac_data'    : 'fac_snapshots',
            'index_cons'  : 'index_snapshots',
        }

        value_type = 0   # usual and default case

        if item_tbl == 'data_items':   # data_items registers raw data
            id, value_type = self.cursor.execute(f"SELECT id, value_type FROM data_items WHERE name=?", name).fetchone()
            tblname, item_id_fld = value_types[value_type], 'item_id'

        else:
            if item_tbl == 'factors':    # factors registers fac_data
                tblname, item_id_fld = 'fac_data', 'fac_id'
            elif item_tbl == 'indexes':    # indexes registers index_cons
                tblname, item_id_fld = 'index_cons', 'index_id'
            else:
                raise ValueError(f'_tblcases: wrong item_tbl: {item_tbl}')

            id = self.cursor.execute(f"SELECT id FROM {item_tbl} WHERE name = ?", name).fetchval()

        if id is None:
            raise ValueError(f'_tblcases: can not find "{name}" in table {item_tbl}')

        if snapshots:
            tblname = snapshot_map[tblname]

        return (id, tblname, item_id_fld, value_type)


    def load(self, names, startdate, enddate, item_tbl, univ_or_insts = None):
        """ load data specified in names from item_tbl for given univ or instruments (unit_or_insts).

            name: a string or list of strings indicating item names to be loaded.
            Returns a dict with key as item names and value the DataFrame.
        """
        if self.inst_ids is None:
            self._preload()

        if type(names) is str:
            names = [names]

        inst_ids = self.get_insts(startdate, enddate, univ_or_insts)

        if not inst_ids:
            return None

        list_str = ','.join((f"'{i}'" for i in inst_ids))  # get each i quoted and join together

        dfs = {}            
        for name in names:
            item_id, tblname, item_id_fld, _ = self._tblcases(name, item_tbl)

            sql = f"""SELECT instrument_id, data_value, data_date FROM {tblname} WHERE {item_id_fld}=?
                    AND data_date >= '{startdate}' AND data_date <= '{enddate}' AND instrument_id in ({list_str})"""

            self.cursor.execute(sql, item_id)   # this way the sql is analized only once
            rows = self.cursor.fetchall()

            df = pd.DataFrame.from_records(rows, columns=['instrument_id', 'data_value', 'data_date'])
            # we can also get column names by [f[0] for f in rows[0].cursor_description]
            df = df.pivot(index='data_date', columns='instrument_id', values='data_value')
            df = df.reindex(columns=inst_ids)
            df.index = pd.to_datetime(df.index)  # convert from object(datetime.date) to pd.DatetimeIndex (datetime64[ns])
            dfs[name] = df
        
        return dfs 

    def save(self, dfs, item_tbl, built_id_fld, built_id = None, snapshots = False):
        """ save a dict of DataFrames (dfs) into db. 

            dfs: dict of DataFrames. Key is used as data item names registered in item_tbl. 
                 Note all dataframes in dfs must come from the same item_tbl.
        """

        if self.inst_ids is None:
            self._preload()

        for name, df in dfs.items():
            if item_tbl == 'rebalances':  # special case: for rebal_weights, rebalances is its item-registry table
                # tricky; item_id of rebal_weights (i.e., rebalance_id) must be passed (as built_id), 
                # so we just do this to make _dump_data work for this case.
                # (usually we find item_id based on item name, but this is not the case here)
                item_id_fld, item_id = built_id_fld, built_id  
                built_id_fld, built_id = None, None
                tblname = 'rebal_weights'
                value_type = 0
            else:
                item_id, tblname, item_id_fld, value_type = self._tblcases(name, item_tbl, snapshots)

            if tblname in ['rawdate_snapshots', 'rawtext_snapshots', 'index_cons', 'rebal_weights']:
                currency = False
            else:
                currency = True

            datafile = f'{self.tmp_data_path}{name}.csv'
            self._dump_data(datafile, name, df, item_id, built_id, currency, value_type)

            upsert_sql = self.upsert_sql.render(
                         tablename=tblname,   datafile=abspath(datafile), 
                         item_id=item_id_fld, built_id=built_id_fld, currency=currency, snapshots=snapshots)

            self.cursor.execute(upsert_sql)
            self.cnxn.commit()


    def load_raw(self, names, startdate, enddate, univ_or_insts = None):
        return self.load(names, startdate, enddate, 'data_items', univ_or_insts)

    def load_index(self, names, startdate, enddate, univ_or_insts = None):
        return self.load(names, startdate, enddate, 'indexes', univ_or_insts)

    def load_univ(self, names, startdate, enddate, univ_or_insts = None):
        """ This exists for backward compatability
            New code should use load_index() instead
        """
        return self.load_index(names, startdate, enddate, univ_or_insts)

    def load_fac(self, names, startdate, enddate, univ_or_insts = None):
        return self.load(names, startdate, enddate, 'factors', univ_or_insts)

    def load_calend(self, exchange, startdate, enddate):
        sql = f"SELECT trading_date FROM calend_dates WHERE exchange=? and trading_date>='{startdate}' and trading_date<='{enddate}'"
        self.cursor.execute(sql, exchange)
        rows = self.cursor.fetchall()
        return pd.to_datetime([row[0] for row in rows])


    def save_fac(self, dfs, built_id):
        """ build_id: fac_construct_id """
        self.save(dfs, 'factors', 'fac_construct_id', built_id)

    def save_fac_snapshots(self, dfs, built_id):
        """ dfs: dict of DataFrames
            build_id: rebal_id 
        """
        self.save(dfs, 'factors', 'rebalance_id', built_id, snapshots=True)

    def save_index_snapshots(self, dfs, built_id):
        """ dfs: dict of DataFrames
            build_id: rebal_id 
        """
        self.save(dfs, 'indexes', 'rebalance_id', built_id, snapshots=True)

    def save_raw_snapshots(self, dfs, built_id):
        """ dfs: dict of DataFrames
            build_id: fac_construct_id or rebalance_id

            NOTE: axctaully we use this for both factor construct and portfolio rebalance.
                  When fac_construct_id is negative, it is actually a rebalance id!
                  We just use the same table for two processes.
        """
        self.save(dfs, 'data_items', 'built_id', built_id, snapshots=True)  

    def save_rebal_weights(self, df, name, built_id):
        """       df: rebalanced portfolio weights, DataFrame
                name: name of the porfolio (our index name)     
            build_id: rebal_id
        """
        self.save({name: df}, 'rebalances', 'rebalance_id', built_id)  # rebalances table for rebal_weights is like 'data_items' for raw_data

    def gen_fac_construct_id(self, info):
        """ Inserts a new entry into fac_construct table and returns the id 
            (referenced as fac_construt_id by other table)
        """
        sql = f"INSERT INTO fac_construct (date, info) VALUES ('{self.u_date}', '{info}') RETURNING id;"
        id = self.cursor.execute(sql).fetchval()
        self.cnxn.commit()
        return id

    def gen_rebal_id(self, 
            fac_construct_id,
            parameter_id,
            index_id,
            impl_date,
            fac_data_date,
            capping_date,
            review_date,
            info):

        """ Inserts a new entry into rebalance table and returns the id 
            (referenced as rebalance_id by other table)
        """

        sql = f"""INSERT INTO rebalance (
            fac_construct_id,parameter_id,index_id,impl_date,fac_data_date,capping_date,review_date,info)
            VALUES ({fac_construct_id},{parameter_id},'{index_id}','{impl_date}','{fac_data_date}','{capping_date if capping_date else ''}','{review_date}','{info}')
            RETURNING id;"""


        id = self.cursor.execute(sql).fetchval()
        self.cnxn.commit()
        return id

    def load_parameter(self, name):
        sql = "SELECT id, info from parameters where name=?"
        p = self.cursor.execute(sql, name).fetchone()
                
        # Currently p.info should be a json string with following fields:
        #   'Pj',
        #   'Qj', 
        #   'max_cap_ratio',
        #   'max_stock_weight',
        #   'max_allow_turnover',
        #   'minimum_weight',
        #   'min_active_cap',
        #   'max_active_cap',
        #   'is_universe_narrowing',
        #   'is_sector_neutral',
        #   'is_active_weight_capping',
        #   'is_stock_screening',
        #   'is_dividend_screening',
        #   'single_or_multiple_narrowing',
        #   'factor_add_or_multiplication',
        #   'narrow_para1',
        #   'narrow_para2', 
        #   'narrow_para3', 
        #   'target_function_case_number',
        #   'screening_factor_list',
        #   'screening_weights',
        #   'screening_delete_percent',
        #   'screening_buffer_percent',
        #   'single_factor_name',
        #   'factor_map_dict',
        #   'factor_direction_dict',
        #   'factor_industry_ignore_dict',
        #   'yield_factor_list',

        r = json.loads(p.info)
        r['id'] = p.id
        return r
  
    def save_parameter(self, name, params):
        if 'id' in params and params['id']:
            sql = f"UPDATE parameters SET (name,info)=('{name}',?) WHERE id={params['id']}"
        else:
            sql = f"INSERT INTO parameters (name,info, update_date) VALUES ('{name}',?,?)"

        info = json.dumps(params)
        self.cursor.execute(sql, info, self.u_date)  
        self.cnxn.commit()


    def load_fx(self, base, quote, startdate, enddate):
        """ load exchange rate of base-quote where

            base: base currency, or denominator
            quote: quote currency, or nominator
        """
        sql = f"SELECT id FROM instruments WHERE type='FOREX'"  # TODO: change to actually used forex type
        rows = self.cursor.execute(sql).fetchall()
        self.cnxn.commit()
   
        pairs = list(rows)
        
        def locate_pairs(base, quote):
            """ Return currency pair, is_inverse """
            if base == quote:  # most likely no such pair in recursion
                return None, False   # recursion exit 

            if f'{base}{quote}' in pairs:
                return [f'{base}{quote}'], [False]
            if f'{quote}{base}' in pairs:
                return [f'{quote}{base}'], [True]
            else:
                p1, inv1 = locate_pairs('USD', base)
                if p1 is None:
                    return None, False
                p2, inv2 = locate_pairs('USD', quote)
                if p2 is None:
                    return None, False

                return [*p1, *p2], [*inv1, *inv2]

        insts, invs = locate_pairs(base, quote)

        dfs = self.load_raw('FOREX', startdate, enddate, insts)

        df0 = dfs[insts[0]]
        if invs[0]:
            df0 = 1 / df0

        if len(dfs) == 1:
            return df0

        df1 = dfs[insts[1]]
        return df0 * df1 if invs[1] else df0 / df1
        
        



        
        
            