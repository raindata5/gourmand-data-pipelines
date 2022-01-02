-- 1) loading business data
-- Alter table public."Business" DROP COLUMN "time_extracted"
-- ALTER TABLE public."Business" ADD COLUMN "time_extracted" timestamp without time zone;
-- Alter table public."Business" alter COLUMN "time_extracted" type timestamp without time zone ;

Alter table public."Business" alter COLUMN "time_extracted" type text;

COPY public."Business" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/yelp_business_01.csv'
WITH CSV DELIMITER '|' NULL '' QUOTE '''';

-- truncate public."Business"

-- select distinct * from public."Business"

-- 2) loading Category data
Alter table public."Categories" alter COLUMN "time_extracted" type text;

COPY public."Categories" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/yelp_cats_01.csv'
WITH CSV DELIMITER '|' NULL '' QUOTE '''';
-- 3) loading County data
Alter table public."County" alter COLUMN "POP" type integer;


COPY public."County" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/census_data_01.csv'
WITH CSV HEADER DELIMITER '|' NULL '' QUOTE '''' ;

truncate public."County"

-- 4) loading Event data
Alter table public."Event" alter COLUMN "time_extracted" type text;

COPY public."Event" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/yelp_events_01.csv'
WITH CSV DELIMITER '|' NULL '' QUOTE '''';

truncate public."Event"




-- 5) loading Review data
Alter table public."Review" alter COLUMN "time_extracted" type text;

COPY public."Review" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/yelp_reviews_01.csv'
WITH CSV HEADER DELIMITER '|' NULL '' QUOTE '''';

-- 6) 
COPY public."StateAbbreviations" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/state_abbreviations.csv'
WITH CSV HEADER DELIMITER '|' NULL '' QUOTE '''';

-- 7)
Alter table public."Transactions" alter COLUMN "time_extracted" type text;

COPY public."Transactions" FROM 
'/home/ubuntucontributor/gourmand-data-pipelines/local_raw_data/extract_2021-12-18/yelp_trans_01.csv'
WITH CSV HEADER DELIMITER '|' NULL '' QUOTE '''';

