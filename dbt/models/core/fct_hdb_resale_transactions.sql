WITH properties AS (
    SELECT * FROM {{ ref('dim_property') }}
),
regions AS (
    SELECT * FROM {{ ref('dim_regions') }}
),
dates AS (
    SELECT * FROM {{ ref('dim_date_spine') }}
),
stg_transactions AS (
    SELECT *,
    ROUND(remaining_lease_years + (COALESCE(remaining_lease_months, 0)/12), 2) AS remaining_lease_years_decimal
    FROM {{ ref('stg_hdb_resale') }}
)

SELECT
    t.unique_id AS transaction_id,
    d.date,
    r.town_id,
    p.storey_id,
    t.block,
    t.street_name,
    t.flat_type,
    t.floor_area_sqm,
    t.flat_model,
    t.lease_commence_date,
    t.remaining_lease,
    CASE
        WHEN t.remaining_lease_years_decimal >= 75 THEN '75+ years'
        WHEN t.remaining_lease_years_decimal >= 60 THEN '60-75 years'
        WHEN t.remaining_lease_years_decimal >= 30 THEN '30-60 years'
        ELSE '0-30 years'
    END AS lease_value_category,
    t.resale_price,
    ROUND(t.resale_price / t.floor_area_sqm, 2) AS price_per_sqm,
    CASE
        WHEN t.resale_price < 400000 THEN 'Budget Range (< $400k)'
        WHEN t.resale_price < 600000 THEN 'Medium Range ($400k-$600k)'
        WHEN t.resale_price < 800000 THEN 'High Range ($600k-$800k)'
        ELSE 'Premium Range ($800k+)'
    END AS price_tier,
FROM stg_transactions t
INNER JOIN dates d
    ON d.date = t.date
LEFT JOIN properties p ON t.storey_range_min = p.storey_range_min
LEFT JOIN regions r ON t.town = r.town
ORDER BY t.date