-- ===================================================== -- Assessment # 1: Retail Sales Database Analysis

-- Submitted By: Ahmed Abdel Kariem

-- =====================================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RetailSales')
BEGIN
    CREATE DATABASE RetailSales
END;
GO

 -- ===================================================== Create the Database
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(25),
    LastName varchar(25),
    Email varchar(255),
    City varchar(50),
    State char(2),
    LoyaltyMember bit
);

CREATE TABLE stores (
    StoreID int PRIMARY KEY,
    StoreName varchar(255),
    StoreCity varchar(255),
    StoreState char(3)
);


CREATE TABLE products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    Category VARCHAR(100) check (Category in ('Clothing', 'Toys', 'Electronics', 'Home', 'Sports', 'Books', 'other', 'others')),
    UnitPrice decimal(12,2)
);

CREATE TABLE sales (
    SaleID INT PRIMARY KEY,
    customerID INT FOREIGN KEY REFERENCES customers(customerID),
    StoreID INT FOREIGN KEY REFERENCES Stores(storeID),
    ProductID INT FOREIGN KEY REFERENCES products(ProductID),
    SaleDate Date,
    Quantity INT,
    TotalAmount Decimal(12,2)
);

 -- ===================================================== Insert Data into the Appropriate Tables

insert into customers (CustomerID, FirstName, LastName, Email, City, State, LoyaltyMember)
select distinct customerID, FirstName, LastName, Email, CustomerCity, CustomerState,
            CASE 
                WHEN LoyaltyMember = '1' THEN 1
                WHEN LoyaltyMember = '0' THEN 0
                WHEN LoyaltyMember IS NULL OR LoyaltyMember = '' THEN NULL
                ELSE NULL
            END
from retail_sales_data

insert into stores
select distinct StoreID, storeName, storecity, storestate from retail_sales_data;

insert into products
select distinct productID, productName, Category, UnitPrice from retail_sales_data;

insert into sales
select saleID, customerID, storeID, productID, saleDate, quantity, totalAmount from retail_sales_data

-- ===================================================== -- Task 1: Count the number of sales by product Category

select p.Category, count(s.saleID) as SaleCount from products p
join sales s on s.productID = p.productID
group by Category

-- ===================================================== -- Task 2: Alter the Sales table to add a Discount column.
Alter TABLE sales add discount DECIMAL(10,2) default 0;
update sales set discount = 0 where discount is null;

select * from sales;

-- ===================================================== -- Task 3: Update the LoyaltyMember status for high-spending customers.

BEGIN TRANSACTION;

create table #premium_members (customerID int PRIMARY Key)

insert into #premium_members
select s.customerID from customers c 
join sales s on s.customerID = c.customerID
group by s.customerID 
having sum(s.totalAmount) > 1000;

IF OBJECT_ID('tempdb..#premium_members') IS NOT NULL
    BEGIN
    update customers set LoyaltyMember = 1 
    where CustomerID in (select CustomerID from #premium_members);

    select count(*) as RecordCount from #premium_members;

    select c.customerID, c.FirstName, c.LastName, c.LoyaltyMember from customers c 
    where c.CustomerID in (select top 5 CustomerID from #premium_members order by customerID asc);
    END;
ROLLBACK;

-- ===================================================== -- Task 4: Find the total sales amount for each store.
select st.storeName, sum(s.TotalAmount) as TotalSales from stores st 
join sales s on s.StoreID = st.StoreID
group by storeName

-- ===================================================== -- Task 5: List the top 5 customers by total purchase amount.
select top 5 c.FirstName, c.LastName, sum(s.totalAmount) as TotalPurchases from customers c 
join sales s on s.customerID = c.CustomerID
group by c.FirstName, c.LastName
order by TotalPurchases DESC;

-- ===================================================== -- Task 6: Find the average sale amount for loyalty members vs. non-members.
select c.LoyaltyMember, avg(s.totalAmount) as AvgSaleAmount from customers c 
join sales s on c.customerID  = s.customerID
group by LoyaltyMember

-- ===================================================== -- Task 7: Create a nonclustered index on the Sales table.
begin TRANSACTION
CREATE NONCLUSTERED INDEX IX_Sales_SaleDate
ON sales(saleDate);
GO

SELECT 
    i.name AS index_name,
    OBJECT_NAME(i.object_id) AS table_name,
    c.name AS column_name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('sales') AND i.name = 'IX_Sales_SaleDate';

ROLLBACK;

-- ===================================================== -- Task 8: Find the store with the highest average sale amount.
select top 1 st.StoreName, avg(s.totalAmount) as AvgSaleAmount from stores st 
join sales s on s.storeID = st.StoreID
group by StoreName
order by AvgSaleAmount desc;

-- ===================================================== -- Task 9: List sales where the total amount exceeds $500.
select s.SaleID, s.CustomerID, p.ProductName, st.StoreName, s.TotalAmount
from sales s
join products p on s.ProductID = p.productID
join stores st on st.storeID = s.StoreID
where s.TotalAmount > 500

-- ===================================================== -- Task 10: Calculate the total quantity sold by state.
select c.State, sum(s.quantity) as TotalQuantity from customers c 
join sales s on s.customerID = s.customerID
group by c.state
GO

-- ===================================================== -- Task 11: Find the top 3 product categories by total sales amount using a CTE.
with SoldCategories AS (
    select p.Category, sum(s.totalAmount) as TotalSales
    from products p
    join sales s on s.ProductID = p.ProductID
    group by Category
)
select top 3  category, TotalSales from SoldCategories order by TotalSales desc

-- ===================================================== -- Task 12: Find customers who made purchases in multiple states.
select distinct c.CustomerID, c.FirstName, c.LastName, count(st.storeState) as StateCount from customers c 
join sales s on s.customerID = c.CustomerID 
join stores st on st.StoreID = s.StoreID
group by c.CustomerID, c.FirstName, c.LastName

-- ===================================================== -- Task 13: Analyze sales trends by month using a temporary table.
create table #MonthlySales (MonthlySalesID int IDENTITY(1,1) PRIMARY key, SaleYear int, SaleMonth int, TotalSales int)

insert into #MonthlySales
select YEAR(saleDate) as SaleYear, MONTH(saleDate) as SaleMonth, sum(TotalAmount) as TotalSales from sales group by YEAR(saleDate), MONTH(saleDate) order by SaleYear desc, SaleMonth

IF OBJECT_ID('tempdb..#MonthlySales') IS NOT NULL
    select * from #MonthlySales
GO

IF OBJECT_ID('tempdb..#MonthlySales') IS NOT NULL
    drop TABLE #MonthlySales
GO

-- ===================================================== -- Task 14: Create a view for customer purchase history.
create view vw_CustomerPurchases AS
select c.*,
s.SaleID,
s.StoreID,
s.SaleDate,
s.Quantity, 
s.totalAmount,
p.*
from customers c 
JOIN sales s on c.customerID = s.customerID
JOIN products p on p.productID = s.productID
GO

IF OBJECT_ID('dbo.vw_CustomerPurchases', 'V') IS NOT NULL
    select top 5 * from vw_CustomerPurchases
GO

IF OBJECT_ID('dbo.vw_CustomerPurchases', 'V') IS NOT NULL
    drop VIEW vw_CustomerPurchases;
GO

-- ===================================================== -- Task 15: Create a stored procedure to retrieve sales by date range.

create PROCEDURE sp_SalesByDateRange
    @StartDate DATE,
    @EndDate DATE
    AS
    BEGIN
    select * from sales
    where saleDate BETWEEN @StartDate and @EndDate
    END;
GO

if OBJECT_ID('sp_SalesByDateRange', 'P') IS NOT NULL
    EXEC sp_SalesByDateRange '2024-01-01', ' 2024-12-31';
GO

if OBJECT_ID('sp_SalesByDateRange', 'P') IS NOT NULL
    DROP PROCEDURE sp_SalesByDateRange;
GO

-- ===================================================== -- Task 16: Generate a comprehensive sales report.
select p.Category, st.StoreState, YEAR(s.SaleDate) as SaleYear, sum(s.TotalAmount) as TotalSalesAmount, sum(s.Quantity) as TotalQuantity from products p
join sales s on s.ProductID = p.ProductID
join stores st on st.StoreID = s.StoreID
group by p.Category, st.StoreState, YEAR(s.SaleDate);
GO

-- uncomment this block if you want to run the file multiple times, if already ran, drop the tables then uncomment the block below.
-- drop table sales;
-- drop table customers;
-- drop table products;
-- drop table stores;