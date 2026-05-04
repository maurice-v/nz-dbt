 

SELECT 'DROP SCHEMA SD00_DATA_SCIENCE.' || schema_name || ' cascade;'--, *
FROM information_schema.schemata
WHERE schema_name like 'TEST%'
ORDER BY schema_name;
