CREATE OR REPLACE TABLE `your-gcp-project-id.insight_ds.orders_summary` AS
SELECT
    OrderID,
    CustomerID,
    ProductCategory,
    Quantity,
    Amount,
    Quantity * Amount AS TotalAmount,
    OrderDate
FROM `your-gcp-project-id.raw_ds.orders_raw`
WHERE OrderID IS NOT NULL;
