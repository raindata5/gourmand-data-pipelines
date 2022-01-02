begin transaction;
-- here we are going to set the new value for the sequence 1 above the current max value but this is only required on the first insert
-- select setval('"_Production"."BusinessHolding2_businessholdingid_seq"',  (SELECT MAX(businessholdingid) FROM "_Production".businessholding));

-- the columns we are going to insert into the table
insert into "_Production".businessholding (businessid, businessrating, reviewcount, closedate)
-- joining the recently inserted data with the businesses we already have in database
with bus_relation as (
	select b.* ,
	b2.businessid
	from public."Business" b
	inner join "_Production".business b2 on b.alias = b2.businessname
)
-- undergoing necessary conversions to insert data
select 
	businessid,
	cast(rating as numeric(2,1)) as businessrating,
	cast(cast(review_count as numeric(30,0)) as int) as reviewcount,
	cast(time_extracted as date) as closedate
from bus_relation;
commit transaction;
