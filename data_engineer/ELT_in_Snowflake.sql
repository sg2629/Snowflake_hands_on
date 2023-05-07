--script in Snowflake

create database AGS_GAME_AUDIENCE;
drop schema AGS_GAME_AUDIENCE.public;
create schema AGS_GAME_AUDIENCE.raw;

create table game_logs(
raw_log variant
);

--S3 bucket
list @uni_kishore/kickoff; 

CREATE FILE FORMAT FF_JSON_LOGS 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = False
ALLOW_DUPLICATE = False
STRIP_OUTER_ARRAY = True
STRIP_NULL_VALUES = False
IGNORE_UTF8_ERRORS = False; 

-- check that both your stage and your file format are working correctly
select $1
from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS );

--Load the File Into The Table
copy into game_logs
from @uni_kishore/kickoff
--files = ( 'author_with_header.json')
file_format = ( format_name=FF_JSON_LOGS );

--Transformation
--Separates Every Attribute into It's Own Column
--select * from game_logs;
create view LOGS as
SELECT 
 raw_log:agent::text as AGENT
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
,raw_log
FROM game_logs;

--Update the File Into The Table
copy into game_logs
from @uni_kishore/updated_feed
--files = ( 'author_with_header.json')
file_format = ( format_name=FF_JSON_LOGS );

create or replace view LOGS as
SELECT 
raw_log:ip_address::text as ip_address
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
,raw_log
FROM game_logs
where RAW_LOG:ip_address::text is not null;

----------------------------------------------------------------------------------
--ETL Enhanced data
----------------------------------------------------------------------------------

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

insert into ags_game_audience.raw.time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');


create schema ENHANCED;

--convert event timestamps from UTC to local
create table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC',timezone,datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, TOD_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu tod
on tod.hour = hour(game_event_ltz)
);

--Productionizing Load
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC',timezone,datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, TOD_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu tod
on tod.hour = hour(game_event_ltz);



--let's truncate so we can start the load over again
-- remember we have that cloned back up so it's fine
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

create or replace task load_logs_enhanced
warehouse = compute_wh
schedule='5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC',timezone,datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, TOD_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu tod
on tod.hour = hour(game_event_ltz)
) as r
ON r.GAMER_NAME = e.GAMER_NAME
and r.game_event_UTC = e.game_event_UTC
and r.game_event_name = e.game_event_name
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
values 
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

--Test
--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 


---------------------------------------------------------------------------------------
--Create Pipeline
---------------------------------------------------------------------------------------
--1.Files Moved Into the Bucket
--2.Create a new stage called UNI_KISHORE_PIPELINE that points to s3://uni-kishore-pipeline. Put this stage in the RAW schema. 
list @UNI_KISHORE_PIPELINE;

create table PIPELINE_LOGS (
RAW_LOG VARIANT
);


copy into PIPELINE_LOGS
from @UNI_KISHORE_PIPELINE
file_format = ( format_name=FF_JSON_LOGS );

create or replace view PL_LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as
SELECT 
 raw_log:ip_address::text as ip_address
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
,raw_log
FROM PIPELINE_LOGS
where RAW_LOG:ip_address::text is not null;

create or replace task load_logs_enhanced
warehouse = compute_wh
schedule='5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC',timezone,datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, TOD_NAME
from PL_LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu tod
on tod.hour = hour(game_event_ltz)
) as r
ON r.GAMER_NAME = e.GAMER_NAME
and r.game_event_UTC = e.game_event_UTC
and r.game_event_name = e.game_event_name
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
values 
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);


create or replace task GET_NEW_FILES
warehouse = compute_wh
schedule='5 minute'
as
copy into PIPELINE_LOGS
from @UNI_KISHORE_PIPELINE
file_format = ( format_name=FF_JSON_LOGS );

EXECUTE TASK GET_NEW_FILES;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

--Keep this code handy for shutting down the tasks each day
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

---------------------------------------------------------------------------------------
--Pipeline improvements
---------------------------------------------------------------------------------------
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

-- create Task-Driven pipeline
CREATE OR REPLACE PIPE GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


create or replace task load_logs_enhanced
warehouse = compute_wh
schedule='5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, convert_timezone('UTC',timezone,datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, TOD_NAME
from ED_PIPELINE_LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu tod
on tod.hour = hour(game_event_ltz)
) as r
ON r.GAMER_NAME = e.GAMER_NAME
and r.game_event_UTC = e.game_event_UTC
and r.game_event_name = e.game_event_name
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
values 
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

