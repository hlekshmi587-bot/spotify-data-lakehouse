-- Staging: country reference data (Silver).
select
    iso_alpha3,
    iso_alpha2,
    country_name,
    iso_numeric_code,
    continent,
    sub_region,
    intermediate_region,
    geography_source,
    has_geography
from {{ source('lakehouse', 'silver_countries') }}
