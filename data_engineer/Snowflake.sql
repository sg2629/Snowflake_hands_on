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

