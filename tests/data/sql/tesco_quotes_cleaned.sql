CREATE OR REPLACE TEMPORARY VIEW tesco_quotes_cleaned AS
SELECT * FROM VALUES
(
    'txn1','gtin1','loc1',to_timestamp('2025-01-03 00:00:00')
)
AS (transaction_number, gtin, location_id, event_timestamp);
