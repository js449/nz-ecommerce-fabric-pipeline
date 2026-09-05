-- ===============================================================================
-- NZ E-COMMERCE SOURCE DATABASE SCHEMA (MS SQL Server / Azure SQL)
-- Purpose: Defines the upstream transactional tables for orders and logistics.
-- ===============================================================================

-- 1. Customers Table (Profile & Regional Shipping Address)
CREATE TABLE DimCustomer (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerGUID UNIQUEIDENTIFIER DEFAULT NEWID(),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NOT NULL,
    City NVARCHAR(50) NOT NULL,
    Region NVARCHAR(50) NOT NULL, -- e.g., 'Auckland', 'Canterbury', 'Waikato'
    PostalCode NVARCHAR(10) NOT NULL,
    CreatedAt DATETIME2 DEFAULT GETUTCDATE()
);

-- 2. Fulfillment Depots Table (Key NZ Logistics Hubs)
CREATE TABLE DimDepot (
    DepotID INT IDENTITY(1,1) PRIMARY KEY,
    DepotCode NVARCHAR(10) NOT NULL, -- e.g., 'DEP-AKL-01', 'DEP-CHC-01'
    DepotName NVARCHAR(100) NOT NULL,
    Island NVARCHAR(20) NOT NULL, -- 'North Island' or 'South Island'
    City NVARCHAR(50) NOT NULL,
    IsActive BIT DEFAULT 1
);

-- 3. Orders Table (Transactional E-Commerce Activity)
CREATE TABLE FactOrders (
    OrderID INT IDENTITY(10001,1) PRIMARY KEY,
    OrderNumber NVARCHAR(20) NOT NULL,
    CustomerID INT NOT NULL,
    FulfillmentDepotID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    RequiredDeliveryDate DATETIME2 NOT NULL,
    OrderStatus NVARCHAR(30) NOT NULL, -- 'Pending', 'Dispatched', 'Delivered', 'Delayed'
    OrderTotalAmount DECIMAL(18,2) NOT NULL,
    LastModifiedDate DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT FK_Orders_Customer FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
    CONSTRAINT FK_Orders_Depot FOREIGN KEY (FulfillmentDepotID) REFERENCES DimDepot(DepotID)
);