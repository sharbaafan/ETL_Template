----------------------------------------Edit by mohamad javad abolfathi-----------------------------
/*
برای دایمنشن ها ETL تمپلیت 
Site:        http://www.NikAmooz.com
Email:       Info@NikAmooz.com
Instagram:   https://instagram.com/nikamooz/
Telegram:	 https://telegram.me/nikamooz
Created By:  Masoud Taheri 
*/
-------------------------------------------------------------------------------- sharbaafan
/*
ایجاد انباره داده
*/
USE master
GO
IF DB_ID('NorthwindDW')>0
BEGIN
	ALTER DATABASE NorthwindDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE NorthwindDW
END
GO
CREATE DATABASE NorthwindDW 
GO
/*
در سرور دیتاویرهاوس StageDW ایجاد 
*/
USE master
GO
IF DB_ID('StageDW')>0
BEGIN
	ALTER DATABASE StageDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE StageDW
END
GO
CREATE DATABASE StageDW 
GO
--------------------------------------------------------------------
/*
این جدول کانفیگ بوده و آخرین رکوردی که خوانده شده در آن نگهداری می شود
*/
USE NorthwindDW
GO
DROP TABLE IF EXISTS ConfigTable
GO
CREATE TABLE ConfigTable
(
	TableName NVARCHAR(100) PRIMARY KEY,
	LastID SQL_VARIANT,
	LastETLTime DATETIME
)
GO
INSERT INTO ConfigTable(TableName,LastID,LastETLTime) VALUES (N'DimCustomer',N'A',GETDATE())
GO
SELECT * FROM ConfigTable
GO
/*
می باشد SCD Type 1 این جدول دارای 
مکانیزم پر شدن آن به صورت زیر است
رکوردهای جدید درج  و رکوردهایی که از قبل وجود دارند آپدیت
*/
USE NorthwindDW
GO
DROP TABLE IF EXISTS DimCustomer
GO
CREATE TABLE DimCustomer
(
	CustomerKey INT IDENTITY PRIMARY KEY,
	CustomerID	NCHAR(5) UNIQUE ,
	CompanyName	NVARCHAR(255),
	ContactName	NVARCHAR(255),
	ContactTitle	NVARCHAR(255),
	Country	NVARCHAR(255),
	City	NVARCHAR(255),
	Region	NVARCHAR(255),
	PostalCode	NVARCHAR(255),
	ETL_Hash VARBINARY(100),
	ETL_Time DATETIME,
	ETL_TimeStamp TIMESTAMP
)WITH(DATA_COMPRESSION=PAGE)
GO
--------------------------------------------------------------------
/*
بدست آوردن آخرین آی دی خوانده شده
*/
USE NorthwindDW
GO
SELECT 
	CAST(LastID AS NCHAR(5)) 
FROM ConfigTable WHERE TableName=N'DimCustomer'
GO
--------------------------------------------------------------------
/*
Stage ساخت جدول 
*/
USE StageDW
GO
DROP TABLE IF EXISTS Stage_DimCustomer
GO
CREATE TABLE Stage_DimCustomer
(
	CustomerID	NCHAR(5) ,
	CompanyName	NVARCHAR(255),
	ContactName	NVARCHAR(255),
	ContactTitle	NVARCHAR(255),
	Country	NVARCHAR(255),
	City	NVARCHAR(255),
	Region	NVARCHAR(255),
	PostalCode	NVARCHAR(255),
	ETL_Hash VARBINARY(100)
)
GO
--ساخت ایندکس کلاستر برای جدول استیج
CREATE UNIQUE CLUSTERED INDEX IX_Clustered ON Stage_DimCustomer(CustomerID)
	WITH(DATA_COMPRESSION=PAGE)
GO
SELECT * FROM Stage_DimCustomer
GO
--------------------------------------------------------------------
USE Northwind
GO
--و درج در استیج OLTP کوئری استخراج داده از سیستم 
SELECT 
	CustomerID,
	CompanyName,	
	ContactName,	
	ContactTitle,
	Country,
	City,
	Region,
	PostalCode	
FROM Customers
WHERE CustomerID>?
GO
--------------------------------------------------------------------
--اگر رکوردهایی در جدول دایمنشن وجود داشته باشد باید آپدیت شوند
USE NorthwindDW
GO
UPDATE NorthwindDW.dbo.DimCustomer SET
	DimCustomer.City=Stage_DimCustomer.City,
	DimCustomer.CompanyName=Stage_DimCustomer.CompanyName,
	DimCustomer.ContactName=Stage_DimCustomer.ContactName,
	DimCustomer.ContactTitle=Stage_DimCustomer.ContactTitle,
	DimCustomer.Country=Stage_DimCustomer.Country,
	DimCustomer.PostalCode=Stage_DimCustomer.PostalCode,
	DimCustomer.Region=Stage_DimCustomer.Region,
	DimCustomer.ETL_Time=GETDATE()
FROM StageDW.dbo.Stage_DimCustomer 
INNER JOIN NorthwindDW.dbo.DimCustomer ON 
	Stage_DimCustomer.CustomerID=DimCustomer.CustomerID AND 
	Stage_DimCustomer.ETL_Hash<>DimCustomer.ETL_Hash
GO
SELECT * FROM Stage_DimCustomer
GO
--------------------------------------------------------------------
--رکوردهایی که در جدول دایمنشن وجود ندارد باید درج شود
USE NorthwindDW
GO
SELECT 
	Stage_DimCustomer.CustomerID,
	Stage_DimCustomer.CompanyName,
	Stage_DimCustomer.ContactName,
	Stage_DimCustomer.ContactTitle,
	Stage_DimCustomer.Country,
	Stage_DimCustomer.City,
	Stage_DimCustomer.Region,
	Stage_DimCustomer.PostalCode,
	Stage_DimCustomer.ETL_Hash
FROM StageDW.dbo.Stage_DimCustomer 
LEFT JOIN NorthwindDW.dbo.DimCustomer ON 
	Stage_DimCustomer.CustomerID=DimCustomer.CustomerID
WHERE DimCustomer.CustomerID IS NULL
GO
--------------------------------------------------------------------
--به روز رسانی آخرین رکوردی که بر روی آن کار شده است
USE NorthwindDW
GO
UPDATE ConfigTable SET 
	LastID=N'A',
	LastETLTime=GETDATE()
WHERE TableName=N'DimCustomer'
GO
SELECT * FROM ConfigTable
SELECT * FROM DimCustomer
--------------------------------------------------------------------
--------------------------------------------------------------------
--------------------------------------------------------------------

SELECT * FROM Northwind..Orders
SELECT * FROM Northwind..[Order Details]


USE Northwind
SP_HELP Orders
SP_HELP 'Order Details'

OrderID	int
CustomerID	nchar
EmployeeID	int
OrderDate	datetime
RequiredDate	datetime
ShippedDate	datetime
ShipVia	int
Freight	money
