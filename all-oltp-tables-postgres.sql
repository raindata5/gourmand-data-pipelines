select 
		QUOTE_IDENT(schemaname) || '.' || QUOTE_IDENT(tablename) as TableName,
		schemaname || '_' || tablename || '.csv' as tablefilename,
		tablename DWHTBL
from pg_catalog.pg_tables
where schemaname in ('_Analytics', '_Production') and tablename <> 'authuser'
order by tablename;