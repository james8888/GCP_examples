USE SCHEMA "FIVETRAN_DATABASE"."AZURE_BLOB_STORAGE";
CREATE OR REPLACE VIEW unpacked_json_dbt AS 
select 
ID,
_modified modified,
f.value:name name,
f.value:email email,
 _FIVETRAN_SYNCED FIVETRAN_SYNCED
from JSONFT,
lateral flatten(input => users ) f;
