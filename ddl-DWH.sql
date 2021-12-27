
--1
-- cci on distancetocounty
-- nonclustered indexes on Citysourcekey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimBusiness](
	[BusinessKey] [bigint] NULL,
	[BusinessSourceKey] [bigint] NULL,
	[BusinessName] [nvarchar](100) NULL,
	[ChainName] [nvarchar](100) NULL,
	[CitySourceKey] [int] NULL,
	[PaymentLevelSymbol] [nvarchar](10) NULL,
	[PaymentLevelName] [nvarchar](100) NULL,
	[Longitude] [decimal](9, 6) NULL,
	[Latitude] [decimal](8, 6) NULL,
	[AddressLine1] [nvarchar](150) NULL,
	[AddressLine2] [nvarchar](100) NULL,
	[AddressLine3] [nvarchar](100) NULL,
	[ZipCode] [varchar](15) NULL,
	[BusinessPhone] [nvarchar](60) NULL,
	[BusinessURL] [nvarchar](500) NULL,
	[is_closed] [int] NULL,
	[DistanceToCounty] [int] NULL,
	[ValidFrom] [datetime] NOT NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimBusiness_cci] ON [_Production].[DimBusiness] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

CREATE CLUSTERED COLUMNSTORE INDEX   
[_Production_DimBusiness_cci]
ON [_Production].[DimBusiness]  
WITH (DROP_EXISTING = ON); 





--2

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimBusinessCategory](
	[BusinessCategoryKey] [bigint] NULL,
	[CategorySourceKey] [bigint] NULL,
	[BusinessCategoryName] [nvarchar](100) NULL,
	[ValidFrom] [datetime] NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimBusinessCategory_cci] ON [_Production].[DimBusinessCategory] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--3
-- NONCLUSTERED IX on BusinessKey, BusinessCategorykey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimBusinessCategoryBridge](
	[BusinessKey] [bigint] NULL,
	[BusinessCategoryKey] [bigint] NULL,
	[BusinessID] [int] NULL,
	[CategoryID] [int] NULL,
	[ValidFrom] [datetime] NOT NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimBusinessCategoryBridge_cci] ON [_Production].[DimBusinessCategoryBridge] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO
--4
-- NONCLUSTERED IX on BusinessKey, TransactionKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimBusinessTransactionBridge](
	[BusinessKey] [bigint] NULL,
	[TransactionKey] [bigint] NULL,
	[BusinessID] [bigint] NULL,
	[TransactionID] [bigint] NULL,
	[ValidFrom] [datetime] NOT NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimBusinessTransactionBridge_cci] ON [_Production].[DimBusinessTransactionBridge] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--5
-- NONCLUSTERED IX on CountySourceKey (maybe)
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimCounty](
	[CountySourceKey] [int] NULL,
	[CountyName] [nvarchar](150) NULL,
	[StateName] [nvarchar](100) NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimCounty_cci] ON [_Production].[DimCounty] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO
--6
-- NONCLUSTERED IX on TheDate
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimDate](
	[TheDate] [date] NULL,
	[TheDay] [int] NULL,
	[TheDayName] [nvarchar](30) NULL,
	[TheWeek] [int] NULL,
	[TheISOWeek] [int] NULL,
	[TheDayOfWeek] [int] NULL,
	[TheMonth] [int] NULL,
	[TheMonthName] [nvarchar](30) NULL,
	[TheQuarter] [int] NULL,
	[TheYear] [int] NULL,
	[TheFirstOfMonth] [date] NULL,
	[TheLastOfYear] [date] NULL,
	[TheDayOfYear] [int] NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimDate_cci] ON [_Production].[DimDate] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--7
-- cci on Attending, CostOfAttending, Interested
-- NONCLUSTERED IX on EventSourceKey , BusinessSourceKey, CitySourceKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimEvent](
	[EventSourceKey] [int] NULL,
	[BusinessSourceKey] [int] NULL,
	[EventName] [nvarchar](400) NULL,
	[Attending] [int] NULL,
	[CostOfAttending] [numeric](16, 1) NULL,
	[is_free] [nvarchar](50) NULL,
	[EventDescription] [nvarchar](500) NULL,
	[Interested] [int] NULL,
	[CitySourceKey] [int] NULL,
	[latitude] [decimal](28, 0) NULL,
	[longitude] [decimal](28, 0) NULL,
	[ZipCode] [varchar](15) NULL,
	[StartTime] [datetimeoffset](0) NULL,
	[EndTime] [datetimeoffset](0) NULL,
	[TicketsUrl] [nvarchar](300) NULL,
	[EventSiteUrl] [nvarchar](300) NULL,
	[CancelDate] [date] NULL,
	[OfficialDate] [date] NULL,
	[CreatedAt] [date] NULL,
	[LastEditedWhen] [datetime] NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimEvent_cci] ON [_Production].[DimEvent] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--8
-- NONCLUSTERED IX on CountySourceKey, CitySourceKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimLocation](
	[CitySourceKey] [bigint] NULL,
	[CityName] [nvarchar](125) NULL,
	[CountyName] [nvarchar](150) NULL,
	[StateName] [nvarchar](100) NULL,
	[CountryName] [nvarchar](100) NULL,
	[CountySourceKey] [int] NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimLocation_cci] ON [_Production].[DimLocation] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--9
-- cci on ReviewRating
-- NONCLUSTERED IX on ReviewKey, ReviewSourceKey, UserKey, BusinessKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimReview](
	[ReviewKey] [bigint] NULL,
	[ReviewSourceKey] [int] NULL,
	[ReviewURL] [nvarchar](300) NULL,
	[ReviewExtract] [nvarchar](600) NULL,
	[ReviewRating] [decimal](28, 0) NULL,
	[CreatedAt] [datetime] NULL,
	[InsertedAt] [datetime] NULL,
	[UserKey] [bigint] NULL,
	[BusinessKey] [bigint] NULL,
	[ValidFrom] [datetime] NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimReview_cci] ON [_Production].[DimReview] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--10
-- NONCLUSTERED IX on TransactionKey, TransactionSourceKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimTransactionType](
	[TransactionKey] [bigint] NULL,
	[TransactionSourceKey] [int] NULL,
	[TransactionName] [varchar](100) NULL,
	[ValidFrom] [datetime] NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimTransactionType_cci] ON [_Production].[DimTransactionType] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--11

-- NONCLUSTERED IX on UserKey, UserSourceKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[DimUser](
	[UserKey] [bigint] NULL,
	[UserSourceKey] [int] NULL,
	[BusinessCategoryName] [nvarchar](450) NULL,
	[UserImageURL] [nvarchar](450) NULL,
	[FirstName] [nvarchar](50) NULL,
	[LastNameInitial] [nvarchar](3) NULL,
	[ValidFrom] [datetime] NULL,
	[ValidTo] [datetime] NULL,
	[is_current] [int] NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_DimUser_cci] ON [_Production].[DimUser] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--12
-- cci on ReviewCount, BusinessRating
-- NONCLUSTERED IX on BusinessSourceKey, BusinessKey,CloseDate
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[FactBusinessHolding](
	[BusinessHoldingID] [bigint] NOT NULL,
	[BusinessKey] [bigint] NULL,
	[BusinessSourceKey] [bigint] NOT NULL,
	[BusinessRating] [decimal](2, 1) NULL,
	[ReviewCount] [int] NULL,
	[CloseDate] [date] NULL,
	[IncrementalCompKey] [varchar](65) NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_FactBusinessHolding_cci] ON [_Production].[FactBusinessHolding] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

--13
-- cci on EstimatedPopulation
-- NONCLUSTERED IX on CountySourceKey
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [_Production].[FactCountyGrowth](
	[CountySourceKey] [int] NULL,
	[EstimationYear] [smallint] NULL,
	[EstimatedPopulation] [int] NULL,
	[LastEditedWhen] [datetime] NULL,
	[IncrementalCompKey] [varchar](19) NOT NULL
) ON [PRIMARY]
GO
CREATE CLUSTERED COLUMNSTORE INDEX [_Production_FactCountyGrowth_cci] ON [_Production].[FactCountyGrowth] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

















DBCC SQLPERF (LOGSPACE);
	GO
DBCC OPENTRAN
GO
SELECT DB_NAME(mf.database_id) database_name,
mf.file_id,
mf.type_desc,
mf.name,
LTRIM(STR(vfs.size_on_disk_bytes/1024.0/1024.0,30,3)) size_mb,
CASE
  WHEN mf.max_size = 0 OR mf.growth = 0 THEN '--'
  WHEN mf.max_size = -1 THEN 'unlimited' 
  ELSE LTRIM(STR(mf.max_size*8192.0/1024.0/1024.0,30,3)) 
END max_size_mb,
CASE WHEN mf.max_size = 0 OR mf.growth = 0 THEN 'none'
ELSE
  CASE mf.is_percent_growth
    WHEN 0 THEN LTRIM(STR(mf.growth*8192.0/1024.0/1024.0,30,3)) +' mb' 
    ELSE LTRIM(STR(mf.growth,4,0)) +'%'
  END 
END growth,
mf.physical_name
FROM master.sys.master_files mf
CROSS APPLY sys.dm_io_virtual_file_stats(mf.database_id,mf.file_id) vfs

select * from BusinessHolding

select distinct SnapshotCompKey from [dbo].[BusinessTransactionBridge]

select * from _Production.DimBusinessTransactionBridge order by BusinessID

select * from _Production.DimBusinessCategoryBridge

select * from _Production.DimTransactionType

select * from _Production.DimBusiness


select * from Snapshots.snap_businesstransactionbridge
