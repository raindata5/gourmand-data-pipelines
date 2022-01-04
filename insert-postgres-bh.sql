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
	br.businessid,
	cast(br.rating as numeric(2,1)) as businessrating,
	cast(cast(br.review_count as numeric(30,0)) as int) as reviewcount,
	cast(br.time_extracted as date) as closedate
from bus_relation br
-- here im making sure for the inserts the data is not already in the table
-- as to prevent duplicates
where exists (select 1 from "_Production".businessholding bh where
				(br.businessid <> bh.businessid) and (cast(br.time_extracted as date) <> bh.closedate));	
commit transaction;

begin transaction;
-- here we're using a cte tp isolate the variables we want
-- through joining the recently inserted data with the businesses we already have 
-- in database
with bus_relation as (
	select b.* ,
	b2.businessid
	from public."Business" b
	inner join "_Production".business b2 on b.alias = b2.businessname
)

update "_Production".businessholding bh
-- undergoing necessary conversions to insert data
set businessrating = cast(br.rating as numeric(2,1)), 
reviewcount = cast(cast(br.review_count as numeric(30,0)) as int)
from bus_relation br
-- data is only getting updated where it's id is already in the db and the close date
-- as well
where (br.businessid = bh.businessid) and (cast(br.time_extracted as date) = bh.closedate)				
returning br.alias;
commit transaction;
