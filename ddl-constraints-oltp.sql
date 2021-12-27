SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[County](
	[CountyID] [bigint] NULL,
	[CountyName] [varchar](100) NULL,
	[StateID] [bigint] NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_County_cci] ON [_Production].[County] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

select * 
from _Production.County ORDER by CountyID ASC

select * FROM _Production.Business


select * from _Analytics.CountyGrowth 


-- changing data types of certain columns in the source tables
SELECT * FROM Transactions
ALTER TABLE dbo.Transactions ALTER COLUMN businessalias NVARCHAR (200);

SELECT * FROM Review
ALTER TABLE dbo.Review ALTER COLUMN [url] NVARCHAR (500);
ALTER TABLE dbo.Review ALTER COLUMN [text] NVARCHAR (500);
ALTER TABLE dbo.Review ALTER COLUMN [user name] NVARCHAR (500);
ALTER TABLE dbo.Review ALTER COLUMN [user image_url] NVARCHAR (500);

SELECT * FROM Event
ALTER TABLE dbo.Event ALTER COLUMN county NVARCHAR (100);
ALTER TABLE dbo.Event ALTER COLUMN [description] NVARCHAR (450);
ALTER TABLE dbo.Event ALTER COLUMN [event_site_url] NVARCHAR (500);
ALTER TABLE dbo.Event ALTER COLUMN [name] NVARCHAR (500);
ALTER TABLE dbo.Event ALTER COLUMN [location address1] NVARCHAR (500);
ALTER TABLE dbo.Event ALTER COLUMN [location city] NVARCHAR (100);

SELECT * FROM County
ALTER TABLE dbo.County ALTER COLUMN county NVARCHAR (100);

SELECT * FROM Categories
ALTER TABLE dbo.Categories ALTER COLUMN alias NVARCHAR (50);
ALTER TABLE dbo.Categories ALTER COLUMN title NVARCHAR (50);
ALTER TABLE dbo.Categories ALTER COLUMN businessalias NVARCHAR (200);

SELECT * FROM Business
ALTER TABLE dbo.Business ALTER COLUMN county NVARCHAR (100);
ALTER TABLE dbo.Business ALTER COLUMN [name] NVARCHAR (300);
ALTER TABLE dbo.Business ALTER COLUMN [location address1] NVARCHAR (500);
ALTER TABLE dbo.Business ALTER COLUMN [location city] NVARCHAR (100);
ALTER TABLE dbo.Business ALTER COLUMN alias NVARCHAR (50);

TRUNCATE table transactions
TRUNCATE table Business
TRUNCATE table categories
TRUNCATE table County
TRUNCATE table [Event]
TRUNCATE table Review
TRUNCATE table stateabbreviations
TRUNCATE table test_table

select * from County


SELECT * 
FROM test_table

insert into StateAbbreviations
VALUES
('Florida','FL'),
('Alaska','AL')

select 'Alaska','AL'
into #test


-- constraints for production tables

-- 1) create new table (constraints and everything)
-- 2) insert data into new table
-- 3) delete old table
-- 4) change name of the new table to reflect the old

-- 1st option to postgres
-- 1) run alembic soudainment
-- 2) then pull all data from sql server 
-- 3) insert data into postgres tables

-- 2nd option to postgres
-- 1) insert data into postgres
-- 2) run dbt
-- 3) then make duplicates of the tables
-- 4) then run alembic
-- 5) insert data from duplicates into original table names


-- CREATE TABLE [_Analytics].[CountyGrowth](
-- 	[CountyID] [bigint] NULL,
-- 	[EstimationYear] [numeric](18, 0) NULL,
-- 	[EstimatedPopulation] [numeric](18, 0) NULL,
-- 	[LastEditedWhen] [datetime] NOT NULL
-- ) ON [PRIMARY]
-- GO


--11

-- create primary key on CountyID, EstimationYear
-- create foreign key on CountyID
CREATE TABLE [_Analytics].[CountyGrowth2](
	[CountyID] [int] not NULL,
	[EstimationYear] [int] not NULL,
	[EstimatedPopulation] [bigint] NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO
-- ALTER TABLE _Analytics.CountyGrowth ALTER COLUMN CountyID int not NULL;  
-- select * from _Analytics.CountyGrowth ORDER by CountyID


INSERT into _Analytics.CountyGrowth2 (CountyID, EstimationYear, EstimatedPopulation, LastEditedWhen)
select 
*
from _Analytics.CountyGrowth cg order by cg.CountyID, cg.EstimationYear


drop TABLE _Analytics.CountyGrowth

EXEC sp_rename '_Analytics.CountyGrowth2', 'CountyGrowth';

ALTER TABLE _Analytics.CountyGrowth
   ADD CONSTRAINT PK_CountyGrowth_CountyID_EstimationYear PRIMARY KEY CLUSTERED (CountyID, EstimationYear);

ALTER TABLE _Analytics.CountyGrowth
   ADD CONSTRAINT FK_CountyGrowth_County FOREIGN KEY (CountyID)
      REFERENCES _Production.County (CountyID)
      ON DELETE CASCADE
;
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Analytics_CountyGrowth_cci] ON [_Analytics].[CountyGrowth] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO
-- why would estimationyear have high selectivity?
-- why would estimatepopulation involve many <> operators

--10
-- create primary key on BusinessID
-- create foreign key on CityID CountyID StateID PaymentLevelID
-- noonclusted index for the foreign keys and referential integrity
drop table _Production.Business2
CREATE TABLE [_Production].[Business2](
	[BusinessID] [int] IDENTITY(1,1),
	[BusinessName] [nvarchar](100) NULL,
	[ChainName] [nvarchar](100) NULL,
	[AddressLine1] [nvarchar](150) NULL,
	[AddressLine2] [varchar](200) NULL,
	[AddressLine3] [varchar](100) NULL,
	[Latitude] [decimal](8, 6) NULL,
	[Longitude] [decimal](9, 6) NULL,
	[ZipCode] VARCHAR(40) NULL,
	[BusinessPhone] [nvarchar](50) NULL,
	[BusinessURL] [varchar](500) NULL,
	[is_closed] [int] NOT NULL,
	[DistanceToCounty] [int] NULL,
	[CityID] [int] NULL,
	[CountyID] [int] NULL,
	[StateID] [int] NULL,
	[PaymentLevelID] [int] NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO
set IDENTITY_INSERT [_Production].[Business2] ON;
INSERT into _Production.Business2(BusinessID, BusinessName, ChainName, AddressLine1, AddressLine2, AddressLine3, Latitude,
Longitude, ZipCode, BusinessPhone, BusinessURL, is_closed, DistanceToCounty, CityID, CountyID,
StateID, PaymentLevelID, LastEditedWhen)
select * 
from _Production.Business order by BusinessID
set IDENTITY_INSERT [_Production].[Business2] OFF;

drop TABLE _Production.Business


EXEC sp_rename '_Production.Business2', 'Business';

ALTER TABLE _Production.Business
   ADD CONSTRAINT PK_Business_BusinessID PRIMARY KEY CLUSTERED (BusinessID);

ALTER TABLE _Production.Business
   ADD CONSTRAINT FK_Business_City FOREIGN KEY (CityID)
      REFERENCES _Production.City (CityID)
      ON DELETE CASCADE
;
ALTER TABLE _Production.Business
   ADD CONSTRAINT FK_Business_County FOREIGN KEY (CountyID)
      REFERENCES _Production.County (CountyID)
      ON DELETE NO ACTION
;
ALTER TABLE _Production.Business
   ADD CONSTRAINT FK_Business_State FOREIGN KEY (StateID)
      REFERENCES _Production.State (StateID)
      ON DELETE NO ACTION
;

ALTER TABLE _Production.Business
   ADD CONSTRAINT FK_Business_PaymentLevel FOREIGN KEY (PaymentLevelID)
      REFERENCES _Production.PaymentLevel (PaymentLevelID)
      ON DELETE CASCADE
;


CREATE NONCLUSTERED INDEX IX_Business_ChainName ON _Production.Business (ChainName)
INCLUDE (BusinessName, CityID, CountyID, StateID, PaymentLevelID);

CREATE NONCLUSTERED INDEX IX_Business_CityID ON _Production.Business (CityID);

CREATE NONCLUSTERED INDEX IX_Business_CountyID ON _Production.Business (CountyID);

CREATE NONCLUSTERED INDEX IX_Business_StateID ON _Production.Business (StateID);

CREATE NONCLUSTERED INDEX IX_Business_PaymentLevelID ON _Production.Business (PaymentLevelID);

--9

-- create primary key on CategoryID
GO
CREATE TABLE [_Production].[BusinessCategory2](
	[CategoryID] [int] identity(1,1) not NULL,
	[CategoryName] [nvarchar](100) UNIQUE not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[BusinessCategory2] ON;
INSERT into _Production.BusinessCategory2(CategoryID, CategoryName, LastEditedWhen)
select * 
from _Production.BusinessCategory order by CategoryID
set IDENTITY_INSERT [_Production].[BusinessCategory2] off;

drop TABLE _Production.BusinessCategory


EXEC sp_rename '_Production.BusinessCategory2', 'BusinessCategory';

ALTER TABLE _Production.BusinessCategory
   ADD CONSTRAINT PK_BusinessCategory_CategoryID PRIMARY KEY CLUSTERED (CategoryID);



--12

-- create primary key on BusinessID CategoryID
-- create foreign key on BusinessID CategoryID

GO
CREATE TABLE [_Production].[BusinessCategoryBridge2](
	[BusinessID] [int] not null,
	[CategoryID] [int] NOT NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[BusinessCategoryBridge2] ON;
INSERT into _Production.BusinessCategoryBridge2 (BusinessID, CategoryID, LastEditedWhen)
select * 
from _Production.BusinessCategoryBridge order by BusinessID, CategoryID

drop TABLE _Production.BusinessCategoryBridge


EXEC sp_rename '_Production.BusinessCategoryBridge2', 'BusinessCategoryBridge';


ALTER TABLE _Production.BusinessCategoryBridge
   ADD CONSTRAINT PK_BusinessCategoryBridge_BusinessID_CategoryID PRIMARY KEY CLUSTERED (BusinessID, CategoryID);

-- begin TRANSACTION
-- delete from _Production.BusinessCategoryBridge 
-- where businessid = 9143
-- COMMIT TRAN
-- ROLLBACK TRANSACTION

-- SELECT *
-- from _Production.BusinessCategoryBridge bcb
-- where not exists (select 1 from _Production.Business b where bcb.businessid=b.businessid)

-- why conflicting?
ALTER TABLE _Production.BusinessCategoryBridge
   ADD CONSTRAINT FK_BusinessCategoryBridge_Business FOREIGN KEY (BusinessID)
      REFERENCES _Production.Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE _Production.BusinessCategoryBridge
   ADD CONSTRAINT FK_BusinessCategoryBridge_BusinessCategory FOREIGN KEY (CategoryID)
      REFERENCES _Production.BusinessCategory (CategoryID)
      ON DELETE CASCADE
;


--13

-- create primary key on BusinessID, CloseDate
-- create foreign key on BusinessID 
-- consideration of not using clustered index on PK? Some cases where better to create a clustered index on another column like maybe if we tend to sort by date
CREATE TABLE [_Production].[BusinessHolding2](
	[BusinessHoldingID] [int] identity(1,1) NOT NULL,
	[BusinessID] [int] NOT NULL,
	[BusinessRating] NUMERIC(2,1) NULL,
	[ReviewCount] [int] NULL,
	[CloseDate] [date] not NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[BusinessHolding2] ON;
INSERT into _Production.BusinessHolding2(BusinessHoldingID, BusinessID, BusinessRating, ReviewCount, CloseDate)
select 
	BusinessHoldingID,
	BusinessID,
	BusinessRating,
	CAST(ReviewCount as numeric(15,0)),
	CloseDate
from _Production.BusinessHolding order by BusinessHoldingID
set IDENTITY_INSERT [_Production].[BusinessHolding2] off;
drop TABLE _Production.BusinessHolding


EXEC sp_rename '_Production.BusinessHolding2', 'BusinessHolding';


ALTER TABLE _Production.BusinessHolding
   ADD CONSTRAINT PK_BusinessHolding_BusinessHoldingID PRIMARY KEY CLUSTERED (BusinessHoldingID);

CREATE NONCLUSTERED INDEX IX_BusinessHolding_BusinessID_SalesYTD ON _Production.BusinessHolding (BusinessID)
INCLUDE (BusinessRating, ReviewCount);

CREATE NONCLUSTERED INDEX IX_BusinessHolding_CloseDate_SalesYTD ON _Production.BusinessHolding (CloseDate)
INCLUDE (BusinessRating, ReviewCount, BusinessID);


-- DELETE from _Production.BusinessHolding
-- where Businessid = 9143

-- select *
-- from _Production.BusinessHolding bh
-- WHERE not exists (select 1 from _Production.Business b where bh.businessid=b.businessid)


ALTER TABLE _Production.BusinessHolding
   ADD CONSTRAINT FK_BusinessHolding_Business FOREIGN KEY (BusinessID)
      REFERENCES _Production.Business (BusinessID)
      ON DELETE CASCADE
;


--14

-- create primary key on BusinessID TransactionID
-- create foreign key on BusinessID TransactionID

GO
CREATE TABLE [_Production].[BusinessTransactionBridge2](
	[BusinessID] [int] not NULL,
	[TransactionID] [int] not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


INSERT into _Production.BusinessTransactionBridge2 (BusinessID, TransactionID, LastEditedWhen)
select * 
from _Production.BusinessTransactionBridge order by BusinessID, TransactionID


drop TABLE _Production.BusinessTransactionBridge


EXEC sp_rename '_Production.BusinessTransactionBridge2', 'BusinessTransactionBridge';

ALTER TABLE _Production.BusinessTransactionBridge
   ADD CONSTRAINT PK_BusinessTransactionBridge_BusinessID_TransactionID PRIMARY KEY CLUSTERED (BusinessID, TransactionID);

ALTER TABLE _Production.BusinessTransactionBridge
   ADD CONSTRAINT FK_BusinessTransactionBridge_Business FOREIGN KEY (BusinessID)
      REFERENCES _Production.Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE _Production.BusinessTransactionBridge
   ADD CONSTRAINT FK_BusinessTransactionBridge_TransactionType FOREIGN KEY (TransactionID)
      REFERENCES _Production.TransactionType (TransactionID)
      ON DELETE CASCADE
;



--8
-- create primary key on CityID
-- create foreign key on CountyID StateID


GO
CREATE TABLE [_Production].[City2](
	[CityID] [int] IDENTITY(1,1) NOT NULL,
	[CityName] [nvarchar](200) not NULL,
	[StateID] [int] NULL,
	[CountyID] [int] NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO

set IDENTITY_INSERT [_Production].[City2] ON;
INSERT into _Production.City2 (CityID, CityName, StateID, CountyID, LastEditedWhen)
select * 
from _Production.City order by CityID

set IDENTITY_INSERT [_Production].[City2] off;
drop TABLE _Production.City


EXEC sp_rename '_Production.City2', 'City';


ALTER TABLE _Production.City
   ADD CONSTRAINT PK_City_CityID PRIMARY KEY CLUSTERED (CityID);

ALTER TABLE _Production.City
   ADD CONSTRAINT FK_City_State FOREIGN KEY (StateID)
      REFERENCES _Production.State (StateID)
      ON DELETE CASCADE
;

ALTER TABLE _Production.City
   ADD CONSTRAINT FK_City_County FOREIGN KEY (CountyID)
      REFERENCES _Production.County (CountyID)
      ON DELETE NO ACTION
	  on UPDATE NO ACTION
-- ;Msg 1785, Level 16, State 0, Line 1
-- Introducing FOREIGN KEY constraint 'FK_City_County' on table 'City' may cause 
-- cycles or multiple cascade paths. Specify ON DELETE NO ACTION or ON UPDATE 
-- NO ACTION, or modify other FOREIGN KEY constraints.


--3

-- create primary key on CountryID


GO
CREATE TABLE [_Production].[Country2](
	[CountryID] [int] IDENTITY(1,1) not NULL,
	[CountryName] [nvarchar](200) unique not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO

set IDENTITY_INSERT [_Production].[Country2] ON;
INSERT into _Production.Country2(CountryID, CountryName, LastEditedWhen)
select * 
from _Production.Country order by CountryID

set IDENTITY_INSERT [_Production].[Country2] off;
drop TABLE _Production.Country


EXEC sp_rename '_Production.Country2', 'Country';


ALTER TABLE _Production.Country
   ADD CONSTRAINT PK_Country_CountryID PRIMARY KEY CLUSTERED (CountryID);


--7

-- create primary key on CountyID
-- create foreign key on StateID

SET QUOTED_IDENTIFIER ON
BEGIN TRANSACTION
GO
CREATE TABLE [_Production].[County2](
	[CountyID] [int] IDENTITY(1,1) unique NOT NULL,
	[CountyName] [varchar](150) not NULL,
	[StateID] [int] not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


GO
set IDENTITY_INSERT [_Production].[County2] ON;
INSERT into _Production.County2 (CountyID, CountyName, StateID, LastEditedWhen)
select * 
from _Production.County order by CountyID
set IDENTITY_INSERT [_Production].[County2] off;
GO
go
drop TABLE _Production.County;
go
GO
EXEC sp_rename '_Production.County2', 'County';
GO
ALTER TABLE _Production.County
   ADD CONSTRAINT PK_County_CountyID PRIMARY KEY CLUSTERED (CountyID);


ALTER TABLE _Production.County
   ADD CONSTRAINT FK_County_State FOREIGN KEY (StateID)
      REFERENCES _Production.State (StateID)
      ON DELETE CASCADE
;





--15


-- create primary key on EventID
-- create foreign key on BusinessID CityID

GO
CREATE TABLE [_Production].[Event2](
	[EventID] [int] IDENTITY(1,1) UNIQUE not NULL,
	[BusinessID] [int] NULL,
	[EventName] [nvarchar](400) NULL,
	[Attending] [int] NULL,
	[CostOfAttending] NUMERIC(16,1) NULL,
	[is_free] [varchar](50) NULL,
	[EventDescription] [nvarchar](400) NULL,
	[Interested] [varchar](400) NULL,
	[CityID] [int] NULL,
	[latitude] [decimal](8, 6) NULL,
	[longitude] [decimal](9, 6) NULL,
	[ZipCode] [nvarchar](50) NULL,
	[StartTime] [datetimeoffset](7) NULL,
	[EndTime] [datetimeoffset](7) NULL,
	[TicketsUrl] [varchar](400) NULL,
	[EventSiteUrl] [varchar](400) NULL,
	[CancelDate] [date] NULL,
	[OfficialDate] [date] NULL,
	[CreatedAt] [date] NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO

UPDATE _Production.Event
set Attending = cast(CAST(Attending as numeric(15,0)) as int), Interested = cast(CAST(Interested as numeric(15,0)) as int)



set IDENTITY_INSERT [_Production].[Event2] ON;
INSERT into _Production.Event2(EventID, BusinessID, EventName, Attending, CostOfAttending, is_free, EventDescription, Interested, 
CityID, latitude, longitude, ZipCode, StartTime, EndTime, TicketsUrl, EventSiteUrl, CancelDate, 
OfficialDate, CreatedAt, LastEditedWhen)
select * 
from _Production.Event order by EventID
set IDENTITY_INSERT [_Production].[Event2] off;

drop TABLE _Production.Event


EXEC sp_rename '_Production.Event2', 'Event';


ALTER TABLE _Production.Event
   ADD CONSTRAINT PK_Event_EventID PRIMARY KEY CLUSTERED (EventID);


ALTER TABLE _Production.Event
   ADD CONSTRAINT FK_Event_Business FOREIGN KEY (BusinessID)
      REFERENCES _Production.Business (BusinessID)
      ON DELETE CASCADE
;

ALTER TABLE _Production.Event
   ADD CONSTRAINT FK_Event_City FOREIGN KEY (CityID)
      REFERENCES _Production.City (CityID)
      ON DELETE no action
;


--6

-- create primary key on EventCategoryID

GO
CREATE TABLE [_Production].[EventCategory2](
	[EventCategoryID] [int] IDENTITY(1,1) NOT NULL,
	[EventCategoryName] [nvarchar](80) unique not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO

set IDENTITY_INSERT [_Production].[EventCategory2] ON;
INSERT into _Production.EventCategory2(EventCategoryID, EventCategoryName, LastEditedWhen)
select * 
from _Production.EventCategory order by EventCategoryID
set IDENTITY_INSERT [_Production].[EventCategory2] off;
drop TABLE _Production.EventCategory


EXEC sp_rename '_Production.EventCategory2', 'EventCategory';

ALTER TABLE _Production.EventCategory
   ADD CONSTRAINT PK_EventCategory_EventCategoryID PRIMARY KEY CLUSTERED (EventCategoryID);



--5

-- create primary key on PaymentLevelID


CREATE TABLE [_Production].[PaymentLevel2](
	[PaymentLevelID] [int] IDENTITY(1,1) NOT NULL,
	[PaymentLevelSymbol] [nvarchar](10) not NULL,
	[PaymentLevelName] [varchar](15) NOT NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[PaymentLevel2] ON;
INSERT into _Production.PaymentLevel2(PaymentLevelID, PaymentLevelSymbol, PaymentLevelName, LastEditedWhen)
select * 
from _Production.PaymentLevel order by PaymentLevelID

drop TABLE _Production.PaymentLevel
set IDENTITY_INSERT [_Production].[PaymentLevel2] OFF;

EXEC sp_rename '_Production.PaymentLevel2', 'PaymentLevel';

ALTER TABLE _Production.PaymentLevel
   ADD CONSTRAINT PK_PaymentLevel_PaymentLevelID PRIMARY KEY CLUSTERED (PaymentLevelID);





--16

-- create primary key on ReviewID
-- create foreign key on UserID BusinessID

drop table _Production.Review2
GO
CREATE TABLE [_Production].[Review2](
	[ReviewID] [int] IDENTITY(1,1) NOT NULL,
	[ReviewURL] [varchar](300) NULL,
	[ReviewExtract] [nvarchar](350) NULL,
	[ReviewRating] [decimal](28, 0) NULL,
	[UserID] [int] not NULL,
	[BusinessID] [int] NULL,
	[CreatedAt] [datetime] not NULL,
	[InsertedAt] [datetime] NOT NULL
) ON [PRIMARY]
GO

GO


set IDENTITY_INSERT [_Production].[Review2] ON;
INSERT into _Production.Review2(ReviewID, ReviewURL, ReviewExtract, ReviewRating, UserID, BusinessID, CreatedAt, InsertedAt)
select * 
from _Production.Review order by ReviewID
set IDENTITY_INSERT [_Production].[Review2] off;
drop TABLE _Production.Review

EXEC sp_rename '_Production.Review2', 'Review';

ALTER TABLE _Production.Review
   ADD CONSTRAINT PK_Review_ReviewID PRIMARY KEY CLUSTERED (ReviewID);


ALTER TABLE _Production.Review
   ADD CONSTRAINT FK_Review_User FOREIGN KEY (UserID)
      REFERENCES _Production.[User] (UserID)
      ON DELETE CASCADE
;

ALTER TABLE _Production.Review
   ADD CONSTRAINT FK_Review_Business FOREIGN KEY (BusinessID)
      REFERENCES _Production.Business (BusinessID)
      ON DELETE CASCADE
;





--4 

-- create primary key on StateID
-- create foreign key on CountryID


GO
CREATE TABLE [_Production].[State2](
	[StateID] [int] IDENTITY(1,1) NOT NULL,
	[StateName] [varchar](60) NULL,
	[AbrvState] [varchar](10) NULL,
	[CountryID] [int] not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO

set IDENTITY_INSERT [_Production].[State2] ON;
INSERT into _Production.State2 (StateID, StateName, AbrvState, CountryID, LastEditedWhen)
select * 
from _Production.State order by StateID
set IDENTITY_INSERT [_Production].[State2] off;

drop TABLE _Production.State


EXEC sp_rename '_Production.State2', 'State';

ALTER TABLE _Production.State
   ADD CONSTRAINT PK_State_StateID PRIMARY KEY CLUSTERED (StateID);


ALTER TABLE _Production.State
   ADD CONSTRAINT FK_State_Country FOREIGN KEY (CountryID)
      REFERENCES _Production.Country (CountryID)
      ON DELETE CASCADE
;

select * from _Production.State2



--15

-- create primary key on TransactionID


GO
CREATE TABLE [_Production].[TransactionType2](
	[TransactionID] [int] IDENTITY(1,1) NOT NULL,
	[TransactionName] [varchar](100) unique not NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[TransactionType2] ON;
INSERT into _Production.TransactionType2 (TransactionID, TransactionName, LastEditedWhen)
select * 
from _Production.TransactionType order by TransactionID
set IDENTITY_INSERT [_Production].[TransactionType2] OFF;
drop TABLE _Production.TransactionType

EXEC sp_rename '_Production.TransactionType2', 'TransactionType';

ALTER TABLE _Production.TransactionType
   ADD CONSTRAINT PK_TransactionType_TransactionID PRIMARY KEY CLUSTERED (TransactionID);





--16 i guess do constraint at the very end

-- create primary key on UserID
begin TRANSACTION
GO
CREATE TABLE [_Production].[User2](
	[UserID] [int] IDENTITY(1,1) NOT NULL,
	[UserProfileURL] [varchar](300) NULL,
	[UserImageURL] [varchar](300) NULL,
	[FirstName] [nvarchar](75) NULL,
	[LastNameInitial] [nvarchar](5) NULL,
	[LastEditedWhen] [datetime] NOT NULL
) ON [PRIMARY]
GO


set IDENTITY_INSERT [_Production].[User2] ON;
INSERT into _Production.User2 (UserID, UserProfileURL, UserImageURL, FirstName, LastNameInitial, LastEditedWhen)
select * 
from _Production.[User] order by UserID

drop TABLE _Production.[User]

EXEC sp_rename '_Production.User2', 'User';


ALTER TABLE _Production.[User]
   ADD CONSTRAINT PK_User_UserID PRIMARY KEY CLUSTERED (UserID);

COMMIT TRANSACTION

drop table _Production.User2
set IDENTITY_INSERT [_Production].[User] OFF;


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

