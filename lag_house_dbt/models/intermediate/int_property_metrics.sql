{{ config(materialized='table') }}

WITH combined_listings AS (
    SELECT * FROM {{ ref('stg_rental_listings') }}
    WHERE
    property_type in (
    "Self Contain (Single Rooms)",
    "Flat / Apartment",
    "Mini Flat (Room and Parlour)",
    "Semi-detached Duplex",
    "Detached Bungalow",
    "Semi-detached Bungalow",
    "Detached Duplex",
    "House",
    "Terraced Duplex",
    "Terraced Bungalow"
        )
    and
    price between 100000 and 150000000
    and 
    bedrooms is not null 
    and 
    bathrooms is not null 
    and 
    toilets is not null
    and 
    lga is not null 
    UNION ALL
    SELECT * FROM {{ ref('stg_sale_listings') }}
    WHERE
    property_type in (
    "Self Contain (Single Rooms)",
    "Flat / Apartment",
    "Mini Flat (Room and Parlour)",
    "Semi-detached Duplex",
    "Detached Bungalow",
    "Semi-detached Bungalow",
    "Detached Duplex",
    "House",
    "Terraced Duplex",
    "Terraced Bungalow"
        )
    and
    price between 100000 and 50000000000
    and 
    bedrooms is not null 
    and 
    bathrooms is not null 
    and 
    toilets is not null
    and 
    lga is not null 
)

SELECT DISTINCT
    lga,
    property_type,
    listing_type,
    AVG(price) avg_price,
    -- PERCENTILE_CONT(price, 0.5) OVER(PARTITION BY lga,property_type, listing_type) AS median_price,
    COUNT(*) listing_count
FROM combined_listings
where bedrooms < 4 and lga not in ('Mushin','Apapa')
GROUP BY 1,2,3
