begin transaction ;
with id_dups as (
select row_number() over (partition by "id" order by "id") dups, b.*
from public."Business" b
) 
select * 
into public."Business2"
from id_dups
where dups = 1;

truncate public."Business";

insert into public."Business" ("id", alias, name, image_url, is_closed,url , 
review_count, rating, price, phone, display_phone, distance, 
"coordinates latitude", "coordinates longitude", "location address1", 
"location address2", "location address3", "location city", "location zip_code"
, "location country", "location state", "county", time_extracted)
select 
	"id", alias, name,image_url, is_closed,url , 
review_count, rating, price, phone, display_phone, distance, 
"coordinates latitude", "coordinates longitude", "location address1", 
"location address2", "location address3", "location city", "location zip_code"
, "location country", "location state", "county", time_extracted
from public."Business2";

drop table public."Business2";

commit transaction;