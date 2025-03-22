-- models/marts/core/dim_location.sql
SELECT DISTINCT
    town_id,
    town,
    region,
    is_mature_estate
FROM {{ ref('town_region_lookup') }}