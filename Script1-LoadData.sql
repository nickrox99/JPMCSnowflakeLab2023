use role dbaxx;
use database dcwt;

use schema schemaxx;

---Create tables to load Campaign Spend 

CREATE or REPLACE TABLE CAMPAIGN_SPEND (
  CAMPAIGN VARCHAR(60), 
  CHANNEL VARCHAR(60),
  DATE DATE,
  TOTAL_CLICKS NUMBER(38,0),
  TOTAL_COST NUMBER(38,0),
  ADS_SERVED NUMBER(38,0)
);


-- Create a Warehouse to load our data 
create or replace warehouse load_whxx
with warehouse_size = 'xsmall'
auto_suspend = 300
auto_resume = true
min_cluster_count = 1
max_cluster_count = 1
scaling_policy = 'standard';


--Let us look at the raw files
ls @dcwt.public.campaign_data_stage;

--Load data into CAMPAIGN_SPEND table

COPY into CAMPAIGN_SPEND
  from @dcwt.public.campaign_data_stage;

----Create table to load semi-structured ClickStream data

CREATE or REPLACE TABLE CLICK_DATA_JSON (
  click_data VARIANT
);

--Have a look at the staged data
ls @dcwt.public.json_data_stage ;

---How many records are we expecting to load
select count($1) from @dcwt.public.json_data_stage;

--Let us load a subset of the data with XS warehouse

COPY into CLICK_DATA_JSON
  from @dcwt.public.json_data_stage
  on_error = 'skip_file'
  PATTERN='.*/.*/data_[0,2,3].*0[.]json';


--Let’s scale our compute UP by increasing our Warehouse size to X-Large:
alter warehouse load_whxx set warehouse_size='large';

-- Now load the remaining files on much larger compute

COPY into CLICK_DATA_JSON
  from @dcwt.public.json_data_stage
  on_error = 'skip_file';


  ---Let us look at the data
select * from CLICK_DATA_JSON limit 10;

--- Let us put some structure around our JSON data
SELECT 
CLICK_DATA:ad_id::string as ad_id,
CLICK_DATA:channel::string as channel,
CLICK_DATA:click::number as click,
CLICK_DATA:cost::float as cost,
CLICK_DATA:ipaddress::string as ipaddress,
CLICK_DATA:macaddress::string as macaddress,
CLICK_DATA:timestamp::number as timestamp
from CLICK_DATA_JSON
limit 10;

---Let run some queries on the JSON data. 
--- Let us find the number of clicks per Channel

select 
CLICK_DATA:channel::string as channel,
sum (CLICK_DATA:click) as num_clicks
from CLICK_DATA_JSON
group by channel
order by num_clicks desc;


---Let us create a structured table from JSON

create or replace view click_data as
SELECT 
CLICK_DATA:ad_id::string as ad_id,
CLICK_DATA:channel::string as channel,
CLICK_DATA:click::number as click,
CLICK_DATA:cost::float as cost,
CLICK_DATA:ipaddress::string as ipaddress,
CLICK_DATA:macaddress::string as macaddress,
CLICK_DATA:timestamp::number as timestamp
from CLICK_DATA_JSON;

--- Grant privileges to our business users to use the tables we just created

grant usage on database dcwt to role analystxx;
grant usage on schema dcwt.schemaxx to role analystxx;
grant select on view click_data to role analystxx;

select * from click_data limit 10;


-----Let appply governance rules 

--ALTER tag PII_data unset masking policy MASK_PII;
--ALTER VIEW CLICK_DATA unset tag PII_data;
--DROP tag IF EXISTS PII; 


create or replace tag PII_Data;


CREATE OR REPLACE masking policy MASK_PII as (val string) returns string ->
  case
    when current_role() IN ('DBAxx') then val
    when current_role() IN ('ANALYSTxx') then '***MASKED***'
  end;
  
ALTER tag PII_Data set masking policy MASK_PII;


ALTER VIEW CLICK_DATA set tag PII_Data = 'tag-based policies';


use role analystxx;

-- All of us now using the same warehouse called Query_WH. Talk about concurrency
-- RBAC allows ANALYST to only use an assigned warehouse.

use warehouse query_wh;

select * from click_data;




