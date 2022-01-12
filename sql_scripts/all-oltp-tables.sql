Select 
  case schema_id
  when 10 then '[_Analytics].' + QUOTENAME(name)
  when 9 then '[_Production].' + QUOTENAME(name)
  end as TableName,
  case schema_id
  when 10 then '[_Analytics]_' + QUOTENAME(name) +'.csv'
  when 9 then '[_Production]_' + QUOTENAME(name) +'.csv'
  end as tablefilename,
  name as DWHTBL
FROM sys.tables
WHERE TYPE = 'U' and schema_id in (9,10)
ORDER BY name