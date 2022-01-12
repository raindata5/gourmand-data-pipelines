-- Table: _Production.Country
--1
-- DROP TABLE "_Production".country;

CREATE TABLE IF NOT EXISTS "_Production".country2
(
    countryid serial PRIMARY KEY,
    countryname text unique not null COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production".country2
    OWNER to postgres;
--

INSERT into "_Production".country2(CountryID, CountryName, LastEditedWhen)
select * 
from "_Production"."Country" order by CountryID 


DROP TABLE "_Production"."Country";

ALTER TABLE IF EXISTS "_Production".country2
RENAME TO country;

ALTER TABLE _Production.Country
   ADD CONSTRAINT PK_Country_CountryID PRIMARY KEY (CountryID);
   
ALTER TABLE "_Production".ountry
        ADD constraint FK_Country_CountryID FOREIGN KEY (event_id) 
		references events(id)
        ;
--
--2
-- DROP TABLE "_Production"."TransactionType";

CREATE TABLE IF NOT EXISTS "_Production".TransactionType2
(
    TransactionID serial primary key ,
    TransactionName text unique not null COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production".TransactionType2
    OWNER to postgres;


INSERT into "_Production".TransactionType2 (TransactionID, TransactionName, LastEditedWhen)
select * 
from "_Production"."TransactionType" order by "TransactionID"


DROP TABLE "_Production"."TransactionType";

ALTER TABLE IF EXISTS "_Production".TransactionType2
RENAME TO TransactionType;



--3

--
-- 

CREATE TABLE IF NOT EXISTS "_Production"."User2"
(
    userid serial primary key,
    userprofileurl character varying(300) COLLATE pg_catalog."default",
    userimageurl character varying(500) COLLATE pg_catalog."default",
    firstname text COLLATE pg_catalog."default",
    lastnameinitial text COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."User2"
    OWNER to postgres;
	
INSERT into "_Production"."User2" (UserID, UserProfileURL, UserImageURL, FirstName, LastNameInitial, LastEditedWhen)
select * 
from "_Production"."User" order by UserID
	
DROP TABLE "_Production"."User";

ALTER TABLE IF EXISTS "_Production"."User2"
RENAME TO "user";

--

--4

-- 

CREATE TABLE IF NOT EXISTS "_Production"."State2"
(
    stateid int,
    statename character varying COLLATE pg_catalog."default",
    abrvstate character varying COLLATE pg_catalog."default",
    countryid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."State2"
    OWNER to postgres;
	
INSERT into "_Production"."State2" (StateID, StateName, AbrvState, CountryID, LastEditedWhen)
select * 
from "_Production"."State" order by StateID

ALTER TABLE IF EXISTS "_Production"."State2"
RENAME TO "state";

DROP TABLE "_Production"."State";

ALTER TABLE "_Production"."state"
   ADD CONSTRAINT PK_State_stateid PRIMARY KEY (stateid)

ALTER TABLE "_Production"."state"
   ADD CONSTRAINT FK_State_Country FOREIGN KEY (CountryID)
      REFERENCES "_Production"."country" (CountryID)
      ON DELETE CASCADE
;


--5

-- DROP TABLE "_Production"."PaymentLevel";

CREATE TABLE IF NOT EXISTS "_Production"."PaymentLevel2"
(
    paymentlevelid serial primary key,
    paymentlevelsymbol text COLLATE pg_catalog."default",
    paymentlevelname text COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)

TABLESPACE pg_default;
ALTER TABLE "_Production"."PaymentLevel2"
    OWNER to postgres;
	
INSERT into "_Production"."PaymentLevel2"(PaymentLevelID, PaymentLevelSymbol, PaymentLevelName, LastEditedWhen)
select * 
from "_Production"."PaymentLevel" order by PaymentLevelID

DROP TABLE "_Production"."PaymentLevel";

ALTER TABLE IF EXISTS "_Production"."PaymentLevel2"
RENAME TO PaymentLevel;

-- 6

-- DROP TABLE "_Production"."EventCategory";

CREATE TABLE IF NOT EXISTS "_Production"."EventCategory2"
(
    eventcategoryid serial primary key,
    eventcategoryname character varying(80) COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."EventCategory2"
    OWNER to postgres;
	
INSERT into "_Production"."EventCategory2"(EventCategoryID, EventCategoryName, LastEditedWhen)
select * 
from "_Production"."EventCategory" order by EventCategoryID

DROP TABLE "_Production"."EventCategory";

ALTER TABLE IF EXISTS "_Production"."EventCategory2"
RENAME TO EventCategory;


--7
-- DROP TABLE "_Production"."County";

CREATE TABLE IF NOT EXISTS "_Production"."County2"
(
    countyid serial primary key,
    countyname character varying(85) COLLATE pg_catalog."default",
    stateid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."County2"
    OWNER to postgres;
	
INSERT into "_Production"."County2" (CountyID, CountyName, StateID, LastEditedWhen)
select * 
from "_Production"."County" order by CountyID

DROP TABLE "_Production"."County";

ALTER TABLE IF EXISTS "_Production"."County2"
RENAME TO County;

ALTER TABLE "_Production".County
   ADD CONSTRAINT FK_County_State FOREIGN KEY (StateID)
      REFERENCES "_Production"."state" (StateID)
      ON DELETE CASCADE
;

--8

-- DROP TABLE "_Production"."City";

CREATE TABLE IF NOT EXISTS "_Production"."City2"
(
    cityid serial primary key,
    cityname text COLLATE pg_catalog."default",
    stateid int,
    countyid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."City2"
    OWNER to postgres;
	
	
INSERT into "_Production"."City2" (CityID, CityName, StateID, CountyID, LastEditedWhen)
select * 
from "_Production"."City" order by CityID


ALTER TABLE IF EXISTS "_Production"."City2"
RENAME TO City;

DROP TABLE "_Production"."City";

ALTER TABLE "_Production".City
   ADD CONSTRAINT FK_City_State FOREIGN KEY (StateID)
      REFERENCES "_Production"."state" (StateID)
      ON DELETE CASCADE
;

ALTER TABLE "_Production".City
   ADD CONSTRAINT FK_City_County FOREIGN KEY (CountyID)
      REFERENCES "_Production".County (CountyID)
      ON DELETE NO ACTION
	  on UPDATE NO ACTION

-- 

--9

-- 

CREATE TABLE IF NOT EXISTS "_Production"."BusinessCategory2"
(
    categoryid serial primary key,
    categoryname text COLLATE pg_catalog."default",
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."BusinessCategory2"
    OWNER to postgres;

INSERT into "_Production"."BusinessCategory2"(CategoryID, CategoryName, LastEditedWhen)
select * 
from "_Production"."BusinessCategory" order by CategoryID

DROP TABLE "_Production"."BusinessCategory";

ALTER TABLE IF EXISTS "_Production"."BusinessCategory2"
RENAME TO BusinessCategory;



--10

-- 

CREATE TABLE IF NOT EXISTS "_Production"."Business2"
(
    businessid serial primary key,
    businessname text COLLATE pg_catalog."default",
    chainname text COLLATE pg_catalog."default",
    addressline1 text COLLATE pg_catalog."default",
    addressline2 character varying(100) COLLATE pg_catalog."default",
    addressline3 character varying(100) COLLATE pg_catalog."default",
    latitude numeric,
    longitude numeric,
    zipcode character varying(50) COLLATE pg_catalog."default",
    businessphone character varying(50) COLLATE pg_catalog."default",
    businessurl character varying(500) COLLATE pg_catalog."default",
    is_closed integer,
    distancetocounty integer,
    cityid int,
    countyid int,
    stateid int,
    paymentlevelid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."Business2"
    OWNER to postgres;
	
-- doesnt work from some reason
-- UPDATE "_Production"."Business"
-- SET "is_closed" = cast("is_closed" as boolean)
-- select cast("is_closed" as boolean) from "_Production"."Business"
--

ALTER TABLE "_Production".business
ALTER COLUMN is_closed TYPE boolean
USING is_closed::boolean;


INSERT into "_Production"."Business2"(BusinessID, BusinessName, ChainName, AddressLine1, AddressLine2, AddressLine3, Latitude,
Longitude, ZipCode, BusinessPhone, BusinessURL, is_closed, DistanceToCounty, CityID, CountyID,
StateID, PaymentLevelID, LastEditedWhen)
select * 
from "_Production"."Business" order by BusinessID

DROP TABLE "_Production"."Business";

ALTER TABLE IF EXISTS "_Production"."Business2"
RENAME TO Business;

ALTER TABLE "_Production".Business
   ADD CONSTRAINT FK_Business_City FOREIGN KEY (CityID)
      REFERENCES "_Production".City (CityID)
      ON DELETE CASCADE
;
ALTER TABLE "_Production".Business
   ADD CONSTRAINT FK_Business_County FOREIGN KEY (CountyID)
      REFERENCES "_Production".County (CountyID)
      ON DELETE NO ACTION
;
ALTER TABLE "_Production".Business
   ADD CONSTRAINT FK_Business_State FOREIGN KEY (StateID)
      REFERENCES "_Production"."state" (StateID)
      ON DELETE NO ACTION
;

ALTER TABLE "_Production".Business
   ADD CONSTRAINT FK_Business_PaymentLevel FOREIGN KEY (PaymentLevelID)
      REFERENCES "_Production".PaymentLevel (PaymentLevelID)
      ON DELETE CASCADE
;


CREATE INDEX IX_Business_ChainName ON "_Production".Business (ChainName)
INCLUDE (BusinessName, CityID, CountyID, StateID, PaymentLevelID);

CREATE INDEX IX_Business_CityID ON "_Production".Business (CityID);

CREATE INDEX IX_Business_CountyID ON "_Production".Business (CountyID);

CREATE INDEX IX_Business_StateID ON "_Production".Business (StateID);

CREATE INDEX IX_Business_PaymentLevelID ON "_Production".Business (PaymentLevelID);

--11

--next page

--


--12

-- 

CREATE TABLE IF NOT EXISTS "_Production"."BusinessCategoryBridge2"
(
    businessid int,
    categoryid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."BusinessCategoryBridge2"
    OWNER to postgres;
	
INSERT into "_Production"."BusinessCategoryBridge2" (BusinessID, CategoryID, LastEditedWhen)
select * 
from "_Production"."BusinessCategoryBridge" order by BusinessID, CategoryID


DROP TABLE "_Production"."BusinessCategoryBridge";

ALTER TABLE IF EXISTS "_Production"."BusinessCategoryBridge2"
RENAME TO BusinessCategoryBridge;

ALTER TABLE "_Production".BusinessCategoryBridge
   ADD CONSTRAINT PK_BusinessCategoryBridge_BusinessID_CategoryID PRIMARY KEY (BusinessID, CategoryID);

ALTER TABLE "_Production".BusinessCategoryBridge
   ADD CONSTRAINT FK_BusinessCategoryBridge_Business FOREIGN KEY (BusinessID)
      REFERENCES "_Production".Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE "_Production".BusinessCategoryBridge
   ADD CONSTRAINT FK_BusinessCategoryBridge_BusinessCategory FOREIGN KEY (CategoryID)
      REFERENCES "_Production".BusinessCategory (CategoryID)
      ON DELETE CASCADE
;

--13
-- DROP TABLE "_Production"."BusinessHolding";

CREATE TABLE IF NOT EXISTS "_Production"."BusinessHolding2"
(
    businessholdingid serial primary key,
    businessid int,
    businessrating NUMERIC(2,1) ,
    reviewcount integer,
    closedate date
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."BusinessHolding2"
    OWNER to postgres;
	
UPDATE "_Production"."BusinessHolding"
set businessrating = CAST(businessrating as numeric(2,1))
	
INSERT into "_Production"."BusinessHolding2"(BusinessHoldingID, BusinessID, BusinessRating, ReviewCount, CloseDate)
select 
	BusinessHoldingID,
	BusinessID,
	CAST(businessrating as numeric(2,1)),
	CAST(ReviewCount as numeric(15,0)),
	CloseDate
from "_Production"."BusinessHolding" order by BusinessHoldingID


DROP TABLE "_Production"."BusinessHolding";

ALTER TABLE IF EXISTS "_Production"."BusinessHolding2"
RENAME TO BusinessHolding;


ALTER TABLE "_Production".BusinessHolding
   ADD CONSTRAINT FK_BusinessHolding_Business FOREIGN KEY (BusinessID)
      REFERENCES "_Production".Business (BusinessID)
      ON DELETE CASCADE
;

CREATE INDEX IX_BusinessHolding_BusinessID ON "_Production".BusinessHolding (BusinessID)
INCLUDE (BusinessRating, ReviewCount);

CREATE INDEX IX_BusinessHolding_CloseDate ON "_Production".BusinessHolding (CloseDate)
INCLUDE (BusinessRating, ReviewCount, BusinessID);

--14

-- DROP TABLE "_Production"."BusinessTransactionBridge";

CREATE TABLE IF NOT EXISTS "_Production"."BusinessTransactionBridge2"
(
    businessid int,
    transactionid int,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."BusinessTransactionBridge2"
    OWNER to postgres;
	
INSERT into "_Production"."BusinessTransactionBridge2" (BusinessID, TransactionID, LastEditedWhen)
select * 
from "_Production"."BusinessTransactionBridge" order by BusinessID, TransactionID


DROP TABLE "_Production"."BusinessTransactionBridge";

ALTER TABLE IF EXISTS "_Production"."BusinessTransactionBridge2"
RENAME TO BusinessTransactionBridge;


ALTER TABLE "_Production".BusinessTransactionBridge
   ADD CONSTRAINT PK_BusinessTransactionBridge_BusinessID_TransactionID PRIMARY KEY (BusinessID, TransactionID);

ALTER TABLE "_Production".BusinessTransactionBridge
   ADD CONSTRAINT FK_BusinessTransactionBridge_Business FOREIGN KEY (BusinessID)
      REFERENCES "_Production".Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE "_Production".BusinessTransactionBridge
   ADD CONSTRAINT FK_BusinessTransactionBridge_TransactionType FOREIGN KEY (TransactionID)
      REFERENCES "_Production".TransactionType (TransactionID)
      ON DELETE CASCADE
;


--15

-- DROP TABLE "_Production"."Event2";

CREATE TABLE IF NOT EXISTS "_Production"."Event2"
(
    eventid serial primary key ,
    businessid int,
    eventname text COLLATE pg_catalog."default",
    attending int ,
    costofattending NUMERIC(16,2),
    is_free boolean ,
    eventdescription text COLLATE pg_catalog."default",
    interested int,
    cityid int,
    latitude numeric(8, 6) ,
    longitude numeric(9, 6) ,
    zipcode character varying(50) COLLATE pg_catalog."default",
    starttime timestamp(3) with time zone,
    endtime timestamp with time zone,
    ticketsurl character varying COLLATE pg_catalog."default",
    eventsiteurl character varying(500) COLLATE pg_catalog."default",
    canceldate date,
    officialdate date,
    createdat date,
    lasteditedwhen timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."Event2"
    OWNER to postgres;
	

-- ALTER TABLE "_Production"."Event"
-- ALTER COLUMN Attending TYPE int
-- USING attending::integer;

-- ALTER TABLE "_Production"."Event"
-- ALTER COLUMN Interested TYPE int
-- USING Interested::integer;

-- ALTER TABLE "_Production"."Event"
-- ALTER COLUMN is_free TYPE boolean
-- USING is_free::boolean;

-- ALTER TABLE "_Production"."Event"
-- ALTER COLUMN latitude TYPE numeric(8, 6)
-- USING latitude::numeric(8, 6);

-- ALTER TABLE "_Production"."Event"
-- ALTER COLUMN longitude TYPE numeric(9, 6)
-- USING longitude::numeric(9, 6);

INSERT into "_Production"."Event2"(EventID, BusinessID, EventName, Attending, CostOfAttending, is_free, EventDescription, Interested, 
CityID, latitude, longitude, ZipCode, StartTime, EndTime, TicketsUrl, EventSiteUrl, CancelDate, 
OfficialDate, CreatedAt, LastEditedWhen)
select * 
from "_Production".Event order by EventID 


DROP TABLE "_Production".Event;

ALTER TABLE IF EXISTS "_Production"."Event2"
RENAME TO "event";


ALTER TABLE "_Production".Event
   ADD CONSTRAINT FK_Event_Business FOREIGN KEY (BusinessID)
      REFERENCES "_Production".Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE "_Production".Event
   ADD CONSTRAINT FK_Event_City FOREIGN KEY (CityID)
      REFERENCES "_Production".City (CityID)
      ON DELETE no action
;

--16

-- 

CREATE TABLE IF NOT EXISTS "_Production"."Review2"
(
    reviewid serial primary key,
    reviewurl character varying(500) COLLATE pg_catalog."default",
    reviewextract text COLLATE pg_catalog."default",
    reviewrating numeric(28,0),
    userid int,
    businessid int,
    createdat date,
    insertedat timestamp(3) without time zone
)
TABLESPACE pg_default;
ALTER TABLE "_Production"."Review2"
    OWNER to postgres;
	
INSERT into "_Production"."Review2"(ReviewID, ReviewURL, ReviewExtract, ReviewRating, UserID, BusinessID, CreatedAt, InsertedAt)
select * 
from "_Production".Review order by ReviewID


DROP TABLE "_Production".Review;

ALTER TABLE IF EXISTS "_Production"."Review2"
RENAME TO Review;



ALTER TABLE "_Production".Review
   ADD CONSTRAINT FK_Review_User FOREIGN KEY (UserID)
      REFERENCES "_Production".User (UserID)
      ON DELETE CASCADE
;

ALTER TABLE "_Production".Review
   ADD CONSTRAINT FK_Review_Business FOREIGN KEY (BusinessID)
      REFERENCES "_Production".Business (BusinessID)
      ON DELETE CASCADE
;


--************
select * from "_Staging"."stg_city" b2
where not exists (select 1 from "_Production"."Business" b where 
				 b.BusinessID = b2.BusinessID)
				 
delete from "_Production"."BusinessCategoryBridge"
where "BusinessCategoryBridge".businessid = 9146