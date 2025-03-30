SELECT
    storey_id,
    storey_range,
    CAST(REGEXP_EXTRACT(storey_range, r'^(\d+)') AS INT64) AS storey_range_min,
    CAST(REGEXP_EXTRACT(storey_range, r'TO (\d+)') AS INT64) AS storey_range_max,
    storey_category
FROM {{ ref('hdb_storey_categorization') }}
