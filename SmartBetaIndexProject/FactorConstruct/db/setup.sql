--
-- Setup and initialize quant_staging database which holds indexes, raw data, and factors. 
--
-- I EXPECT a slight midification to this file for using in teradata.

-- For a new postgresql installation or a new refresh start of quant_staging, run this from command line:
--     psql -f setup.sql postgres   
-- where postgres is the default db for postgresql which must be specified though we will create our own
-- database quant_staging.

DROP DATABASE IF EXISTS quant_staging;

-- DROP USER pliu;
-- CREATE USER pliu WITH PASSWORD '1234';
-- ALTER ROLE pliu WITH SUPERUSER;

CREATE DATABASE quant_staging;

-- select the created database; may need to change to like `USE quant_staging`
\c quant_staging;   

---------------------------------------------------------------------------------------------------
-- Table: instruments
-- 
-- Registring all instruments, e.g., stocks, interest rates, forex rates, bonds, futures, 
--   options etc.
---------------------------------------------------------------------------------------------------
CREATE TABLE instruments (
  id VARCHAR(20) PRIMARY KEY,         -- unique id for the instrument; using facset id
  name VARCHAR(60),                   -- name of the instrument
  exchange VARCHAR(20) DEFAULT NULL,  -- market; prefer a standard code if there exists one
  start_date DATE,
  end_date DATE,
  type VARCHAR(6),                    -- indicates type type of the inst, e.g., stock, fotex, interest rates, etc. Prefer using standard code if there exits
  update_date DATE NOT NULL      -- time when the this record updated
);

CREATE INDEX ON instruments (id);

---------------------------------------------------------------------------------------------------
-- Table: data_items
--
-- For registering all `raw` data items (data directly coming from data vendors or markets).
---------------------------------------------------------------------------------------------------
CREATE TABLE data_items (
  id SERIAL PRIMARY KEY,                     -- unique id for the item; may use external one if such id exsits
  name VARCHAR(60) UNIQUE NOT NULL,          -- meaningful name for the data item; e.g., IBEMS_FY1_NUMUP_EPS
  value_type SMALLINT NOT NULL DEFAULT 0,    -- 0: double, 1: text, 2: date
  source VARCHAR(255) DEFAULT NULL,          -- data source, e.g., FactSet:xxxx; or could be an URL for fetch data
  description VARCHAR(255) DEFAULT NULL,     -- any comments about this item that not reflected from its name or source
  currency_treatment smallint DEFAULT 0      -- 0: do nothing, 1: price conversion, 2: return conversion
);

---------------------------------------------------------------------------------------------------
-- Table: index
--
-- For registering all index (e.g., HSCI); having the same structure as data_items.
-- We can consider it as some special data items separately listed here.
---------------------------------------------------------------------------------------------------
CREATE TABLE indexes (
  id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(60) UNIQUE NOT NULL,
  start_date DATE,     -- if NULL, treat it since the very beginning
  end_date DATE,       -- if NULL, means present
  is_future boolean,        -- true for index that will effect in a near future (but not now)     
  source VARCHAR(255) DEFAULT NULL,
  description VARCHAR(255) DEFAULT NULL
);

---------------------------------------------------------------------------------------------------
-- Table: fac_construct
--
-- Keep records for every factor construction and its parameters.
---------------------------------------------------------------------------------------------------
CREATE TABLE fac_construct (
  id SERIAL PRIMARY KEY,  -- id auto generated for this factor construction
  date DATE NOT NULL,     -- when the construction is performed
  info VARCHAR(4096)      -- info from which we can recovery context and parameters running fac construct
);


---------------------------------------------------------------------------------------------------
-- Table: parameters
--
-- Columns represent parameter, each row represents a set of parameters which would be referenced
-- by rebalnce
---------------------------------------------------------------------------------------------------
CREATE TABLE parameters (
  id SERIAL PRIMARY KEY,            -- id auto generated for this set of parameters
  name VARCHAR(60) UNIQUE NOT NULL,
  info VARCHAR(4096) NOT NULL, 
    -- info is a json string containing:
    --   Pj double precision,
    --   Qj double precision, 
    --   max_cap_ratio	double precision,
    --   max_stock_weight double precision,
    --   max_allow_turnover double precision,
    --   minimum_weight double precision,
    --   min_active_cap double precision,
    --   max_active_cap	double precision,
    --   is_universe_narrowing	boolean,
    --   is_sector_neutral	boolean,
    --   is_active_weight_capping boolean,
    --   is_stock_screening boolean,
    --   is_dividend_screening boolean,
    --   single_or_multiple_narrowing VARCHAR(20),
    --   factor_add_or_multiplication	VARCHAR(20),
    --   narrow_para1 	double precision,
    --   narrow_para2	double precision, 
    --   narrow_para3	double precision, 
    --   target_function_case_number	INTEGER, 
    --   screening_factor_list VARCHAR(512),
    --   screening_weights	VARCHAR(512),
    --   screening_delete_percent double precision,
    --   screening_buffer_percent double precision,
    --   single_factor_name VARCHAR(32),
    --   factor_map_dict	VARCHAR(512),
    --   factor_direction_dict VARCHAR(1024),
    --   factor_industry_ignore_dict	VARCHAR(512),
    --   yield_factor_list	VARCHAR(512)

  update_date DATE NOT NULL
);

CREATE INDEX ON parameters(name);


---------------------------------------------------------------------------------------------------
-- Table: rebalance
--
-- Keep records for every rebalance and its parameters.
---------------------------------------------------------------------------------------------------
CREATE TABLE rebalance (
  id SERIAL PRIMARY KEY,           -- id auto generated for this rebalance
  fac_construct_id INTEGER NOT NULL REFERENCES fac_construct,
  parameter_id INTEGER NOT NULL,   -- REFERENCES parameters,
  index_id VARCHAR(20) NOT NULL REFERENCES indexes,
  impl_date DATE NOT NULL,
  fac_data_date DATE NOT NULL, -- date of factor data for this rebalance
  capping_date DATE,
  review_date DATE,
  info VARCHAR(4096)
);


---------------------------------------------------------------------------------------------------
-- Table: industry
--
-- Keep industry information; i.e., mapping between industry code and its meaningful names.
---------------------------------------------------------------------------------------------------
CREATE TABLE industry (
  item_id INTEGER NOT NULL REFERENCES data_items, -- every/each kind of industry is registered as one item in data_item 
                                    --   e.g., 58 for HSICS, 59 for GICS, etc. 
  code INTEGER NOT NULL,            -- code, e.g, 10, 15, 20, etc.
  level_n SMALLINT,                 -- some industry code system may be sub-divided into several levels;
                                    --    e.g., for GICS, 3 levels: sector, industry group, industry.                                 
  level VARCHAR(30),                -- level description, e.g., industry, or, taking GICS as an example, sector, industry group, industry.
  short_name VARCHAR(12),           -- names for showing in graphs and charts, so keep them shorter
  full_name VARCHAR(100) NOT NULL,  -- descriptional name for the level corresponding to code; e.g., Energy, Utilities, etc.
  start_date DATE,
  end_date DATE
);

---------------------------------------------------------------------------------------------------
-- Table: security_mapping
--
-- Keep an security id mapping between factset id and market symbols
---------------------------------------------------------------------------------------------------
CREATE TABLE security_mapping (
  factset_id VARCHAR(20) NOT NULL REFERENCES instruments, -- may need to change type based on the real type of factset Id
  mapped_symbol VARCHAR(30) NOT NULL,      -- mapped security symbol
  mapping_type SMALLINT NOT NULL,          -- e.g., 0 for market symbol, 1 RIC code, etc.
  start_date DATE,                    -- effective date; if NULL, assume effected since the very beginning
  end_date DATE                       -- end of effective date (kinda expiration); NULL means presently in effect
);

CREATE INDEX ON security_mapping(factset_id);

---------------------------------------------------------------------------------------------------
-- Table: factors
--
-- For registering all factors (data calculated from a group of raw data items).
-- It has exactly the same structur as data_item, but used for registring factors.
-- Likely, as mentionly below, fac_data table would have the same structure as raw_data table,
-- but for factor time series data.
---------------------------------------------------------------------------------------------------
CREATE TABLE factors (
  id SERIAL PRIMARY KEY,                 -- unique id automately generated on initialization; not changed thereafter
  name VARCHAR(60) UNIQUE NOT NULL,      -- descriptional name of the factor, e.g., BETA_D
  source VARCHAR(255) DEFAULT NULL,      -- data source; e.g., function name for calculating it
  description VARCHAR(255) DEFAULT NULL, -- any comments not reflected in other fields
  catogetory character(1)                          -- something like Quality, Growth, Momentum, etc. 
);


---------------------------------------------------------------------------------------------------
-- Table: raw_data
--
-- Store all time series data for items registered in `data_items` table.
-- Therefore the whole table keeps data for all data items (registered in table `data_item`)
-- and for all related instruments (registered in table `instruments`)
--
-- Note that we have 2 foreign keys and a unique constraints.
-- If our data processing process can guarantee these constraints, we can remove them from table 
-- definition.
---------------------------------------------------------------------------------------------------
CREATE TABLE raw_data (         
  item_id INTEGER NOT NULL REFERENCES data_items, -- date item id registered in `data_items`
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments, -- instrument id registered in `instruments`
  data_date DATE NOT NULL,     -- date corresponding to the data value; resolution specfic to the date item
  data_value double precision,      -- numerical data value 
  currency char(3),                 -- currency for data_value if appropriate; otherwise leave it as NULL
  update_date DATE NOT NULL,   -- date when the data populated to this table, or update date from source if source has such a field
  UNIQUE(item_id, instrument_id, data_date) -- with this constraint, data with the same trio element should be updated instead of appended
);

---------------------------------------------------------------------------------------------------
-- Indexes of table raw_data
-- We expect raw_data would be very big, so some indexes is necessary
---------------------------------------------------------------------------------------------------
CREATE INDEX ON raw_data (item_id);
CREATE INDEX ON raw_data (instrument_id);
CREATE INDEX ON raw_data (data_date);


---------------------------------------------------------------------------------------------------
-- Table: fac_data
--
-- Store all time series data for factors registered in `factors` table.
-- Therefore the whole table keeps data of all factors (registered in table `factors`)
-- and for all related instruments (registered in table `instruments`)
--
-- Note that we have 2 foreign keys and a unique constraints.
-- If our data processing process can guarantee these constraints, we can remove them from table 
-- definition.
---------------------------------------------------------------------------------------------------
CREATE TABLE fac_data (            -- just like raw_data
  fac_construct_id INTEGER NOT NULL REFERENCES fac_construct,
  fac_id INTEGER NOT NULL REFERENCES factors, -- factor id registered in `factors`
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments, -- instrument id registered in `instruments`
  data_date DATE NOT NULL,     -- date corresponding to the data value; resolution specfic to the date item
  data_value double precision,      -- numerical data value 
  currency char(3),                 -- currency for data_value if appropriate; otherwise leave it as NULL
  update_date DATE NOT NULL,   -- date when the data populated to this table, or update date from source if source has such a field
  UNIQUE(fac_id, instrument_id, data_date) -- with this constraint, data with the same trio element should be updated instead of appended
);

CREATE INDEX ON fac_data (fac_id);
CREATE INDEX ON fac_data (instrument_id);
CREATE INDEX ON fac_data (data_date);
CREATE INDEX ON fac_data (fac_construct_id);  -- is it necessary

---------------------------------------------------------------------------------------------------
-- Table: index_cons  (index constituents)
--
-- Store all time series weighting data for index registered in `index` table.
-- Therefore the whole table keeps data of all indexes (registered in table `index`)
-- and for all related instruments (registered in table `instruments`)
--
-- Note that we have 2 foreign keys and a unique constraints.
-- If our data processing process can guarantee these constraints, we can remove them from table 
-- definition.
---------------------------------------------------------------------------------------------------

CREATE TABLE index_cons (
  index_id VARCHAR(20) NOT NULL REFERENCES indexes, -- date item id registered in `indexes`
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments, -- instrument id registered in `instruments`
  data_date DATE NOT NULL,     -- date corresponding to the data value; resolution specfic to the date item
  data_value double precision,      -- numerical data value 
  currency char(3),                 -- currency for data_value if appropriate; otherwise leave it as NULL
  update_date DATE NOT NULL,   -- date when the data populated to this table, or update date from source if source has such a field
  UNIQUE(index_id, instrument_id, data_date) -- with this constraint, data with the same trio element should be updated instead of appended
);


---------------------------------------------------------------------------------------------------
-- Table: raw_textdata,
--
-- Similiar to raw_data, for data with textual values.
-- When cncounter textual values, however, we should consider if it can be numericalized in an
-- easy or existing way
-- Currently not used!
---------------------------------------------------------------------------------------------------
CREATE TABLE raw_textdata (
  item_id INTEGER REFERENCES data_items,
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments,
  data_date DATE NOT NULL,
  data_value VARCHAR(255),    -- different from table `raw_data`; and no currency fields
  update_date DATE NOT NULL,
  UNIQUE(item_id, instrument_id, data_date)
);

-- raw_textdata indexes
CREATE INDEX ON raw_textdata (item_id);
CREATE INDEX ON raw_textdata (instrument_id);
CREATE INDEX ON raw_textdata (data_date);


---------------------------------------------------------------------------------------------------
-- Table: raw_datedata,
--
-- Similiar to raw_data, for data with textual values.
-- When cncounter textual values, however, we should consider if it can be numericalized in an
-- easy or existing way
-- Currently not used!
---------------------------------------------------------------------------------------------------
CREATE TABLE raw_datedata (
  item_id INTEGER REFERENCES data_items,
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments,
  data_date DATE NOT NULL,
  data_value DATE,    -- different from table `raw_data`
  update_date DATE NOT NULL,
  UNIQUE(item_id, instrument_id, data_date)
);

-- raw_textdata indexes
CREATE INDEX ON raw_datedata (item_id);
CREATE INDEX ON raw_datedata (instrument_id);
CREATE INDEX ON raw_datedata (data_date);


---------------------------------------------------------------------------------------------------
-- Snapshots Tables:
--
-- Storing all data used in a production rebalance and factor construction.
-- From snapshots tables, we should be able to reproduce all rebalance results anytime. 
-- NOTE ALL SNAPSHOTS TABLES HAVE NO EXTERNAL REFERENCES.
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Table: fac_snapshots
--
-- Snapshots of fac_data.
---------------------------------------------------------------------------------------------------
CREATE TABLE fac_snapshots (
  rebalance_id INTEGER NOT NULL,         -- reference id in rebalance
  fac_id INTEGER NOT NULL,               -- should be referenced to table factors but unnecessary in pratice
  instrument_id VARCHAR(20) NOT NULL,    -- should be referenced to table instruments but unnecessary in pratice
  data_date DATE NOT NULL,
  data_value double precision,
  currency char(3),
  update_date DATE NOT NULL,
  UNIQUE(rebalance_id, fac_id, instrument_id, data_date) -- Note the constraint has update_date in, differ from raw_data
);

-- fac_snapshots indexes
CREATE INDEX ON fac_snapshots (rebalance_id);
CREATE INDEX ON fac_snapshots (fac_id);


---------------------------------------------------------------------------------------------------
-- Table: index_snapshots
--
-- Snapshots of table index_cons
---------------------------------------------------------------------------------------------------
CREATE TABLE index_snapshots (
  rebalance_id INTEGER,         -- reference to id in rebalance table by code
  index_id VARCHAR(20),         -- should be referenced to table factors but unnecessary in pratice
  instrument_id VARCHAR(20),    -- should be referenced to table instruments but unnecessary in pratice
  data_date DATE NOT NULL,
  data_value double precision,
  currency char(3),
  update_date DATE NOT NULL,
  UNIQUE(rebalance_id, index_id, instrument_id, data_date) -- Note the constraint has update_date in, differ from raw_data
);

CREATE INDEX ON index_snapshots (rebalance_id);
CREATE INDEX ON index_snapshots (index_id);


---------------------------------------------------------------------------------------------------
-- Table: raw_snapshots
--
-- Snapshots of table raw_data
---------------------------------------------------------------------------------------------------
CREATE TABLE raw_snapshots (
  built_id INTEGER NOT NULL,          -- if positive, it is a fac_construct_id; negative for rebalance_id
  item_id INTEGER NOT NULL,           -- should be referenced to table factors but unnecessary in pratice
  instrument_id VARCHAR(20) NOT NULL, -- should be referenced to table instruments but unnecessary in pratice
  data_date DATE NOT NULL,
  data_value double precision,
  currency char(3),
  update_date DATE NOT NULL,
  UNIQUE(built_id, item_id, instrument_id, data_date) 
);

CREATE INDEX ON raw_snapshots (built_id);
CREATE INDEX ON raw_snapshots (item_id);

---------------------------------------------------------------------------------------------------
-- Table: rawtext_snapshots
--
-- Snapshots of table raw_data
---------------------------------------------------------------------------------------------------
CREATE TABLE rawtext_snapshots (
  built_id INTEGER NOT NULL,          -- if positive, it is a fac_construct_id; negative for rebalance_id
  item_id INTEGER NOT NULL,           -- should be referenced to table factors but unnecessary in pratice
  instrument_id VARCHAR(20) NOT NULL, -- should be referenced to table instruments but unnecessary in pratice
  data_date DATE NOT NULL,
  data_value VARCHAR(255),
  update_date DATE NOT NULL,
  UNIQUE(built_id, item_id, instrument_id, data_date) 
);

-- rawtext_snapshots indexes
CREATE INDEX ON rawtext_snapshots (built_id);
CREATE INDEX ON rawtext_snapshots (item_id);


---------------------------------------------------------------------------------------------------
-- Table: rawdate_snapshots
--
-- Snapshots of table rawtext_data
---------------------------------------------------------------------------------------------------
CREATE TABLE rawdate_snapshots (
  built_id INTEGER NOT NULL,            -- if positive, it is a fac_construct_id; negative for rebalance_id
  item_id INTEGER NOT NULL,             -- should be referenced to table factors but unnecessary in pratice
  instrument_id VARCHAR(20) NOT NULL,   -- should be referenced to table instruments but unnecessary in pratice
  data_date DATE NOT NULL,
  data_value DATE,
  update_date DATE NOT NULL,
  UNIQUE(built_id, item_id, instrument_id, data_date) 
);

-- rawdate_snapshots indexes
CREATE INDEX ON rawdate_snapshots (built_id);
CREATE INDEX ON rawdate_snapshots (item_id);


---------------------------------------------------------------------------------------------------
-- Table: rebal_weights
--
-- Results of rebalance
---------------------------------------------------------------------------------------------------
CREATE TABLE rebal_weights (
  rebalance_id INTEGER NOT NULL REFERENCES rebalance,
  instrument_id VARCHAR(20) NOT NULL REFERENCES instruments,
  data_date DATE NOT NULL,
  data_value double precision NOT NULL,  -- weights
  update_date DATE NOT NULL,
  UNIQUE(rebalance_id, instrument_id, data_date)
);

CREATE INDEX ON rebal_weights (rebalance_id);


CREATE TABLE calend_dates (
  exchange VARCHAR(20) NOT NULL,
  trading_date DATE NOT NULL
);

CREATE INDEX ON calend_dates (exchange);

GRANT SELECT,
  INSERT,
  UPDATE,
  DELETE
  ON ALL TABLES IN SCHEMA PUBLIC TO pliu;
