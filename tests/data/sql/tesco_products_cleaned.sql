CREATE OR REPLACE TEMPORARY VIEW tesco_products_cleaned AS
SELECT
    * FROM VALUES
(
    'prod1', 'item1', 'gtin1', 'tpna1', 'tpnb1', 'tpnc1',
    'Product 1', 'Brand 1', 'C1', 'Class 1', 1,
    'D1', 'Dept 1', 1, 'DIV1', 'Division 1', 1,
    'SEC1', 'Section 1', 1, 'SC1', 'Subclass 1', 1,
    TIMESTAMP('2025-01-01 00:00:00'), TIMESTAMP('2025-01-02 00:00:00'),
    NAMED_STRUCT(
        'file_path', '', 'file_name', '', 'file_size', 0,
        'file_block_start', 0, 'file_block_length', 0,
        'file_modification_time', NULL,
        'bronze_ingested_at', NULL, 'silver_processed_at', NULL
    )
)
    AS (
        product_id, item_number, gtin, tpna, tpnb, tpnc,
        description, brand, class_code, class_name, class_number,
        department_code, department_name, department_number,
        division_code, division_name, division_number,
        section_code, section_name, section_number,
        subclass_code, subclass_name, subclass_number,
        created_datetime_utc, last_modified_datetime_utc,
        _metadata
    );
