SELECT
    unique_id,
    row_md5sum,
    row_counter,
    month,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', month)) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', month)) AS month_number,
    town,
    flat_type,
    block,
    street_name,
    storey_range,
    CAST(REGEXP_EXTRACT(storey_range, r'^(\d+)') AS INT64) AS storey_range_min,
    CAST(REGEXP_EXTRACT(storey_range, r'TO (\d+)') AS INT64) AS storey_range_max,
    floor_area_sqm,
    flat_model,
    lease_commence_date,
    remaining_lease,
    CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) years') AS INT64) AS remaining_lease_years,
    CASE 
        WHEN REGEXP_CONTAINS(remaining_lease, r'(\d+) months?')
        THEN CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) months?') AS INT64)
        ELSE 0
    END AS remaining_lease_months,
    resale_price
FROM {{ source("staging", "hdb_resale") }}