CREATE OR REPLACE TEMPORARY VIEW tesco_locations_cleaned AS
SELECT
    * FROM VALUES
(
    'loc1', 'Location 1', 'GB', 'SF1', 'DSF1',
    TIMESTAMP('2025-01-01 00:00:00'),
    TIMESTAMP('2025-01-02 00:00:00'),
    NAMED_STRUCT(
        'file_path', '', 'file_name', '', 'file_size', 0,
        'file_block_start', 0, 'file_block_length', 0,
        'file_modification_time', NULL,
        'bronze_ingested_at', NULL, 'silver_processed_at', NULL
    )
)
    AS (
        location_id, location_name, country_code,
        store_format, detailed_store_format,
        created_at, modified_at, _metadata
    );
