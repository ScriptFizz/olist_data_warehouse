-- Grain: one row per postal-code prefix.
-- The canonical city/state is the most frequently observed combination, with deterministic alphabetical tie-breaking. Coordinates use the mean of available observations.

with geolocation as (

    select *
    from {{ ref('stg_geolocation') }}

),

location_counts as (

    select
        geo_zip_code_prefix,
        geo_state_code,
        geo_city,
        count(*) as location_observation_count
    from geolocation
    group by
        geo_zip_code_prefix,
        geo_state_code,
        geo_city

),

ranked_locations as (

    select
        geo_zip_code_prefix,
        geo_state_code,
        geo_city,
        location_observation_count,

        row_number() over (
            partition by geo_zip_code_prefix
            order by
                location_observation_count desc,
                geo_state_code,
                geo_city
        ) as location_rank
    from location_counts

),

coordinates as (

    select
        geo_zip_code_prefix,
        avg(latitude) as latitude,
        avg(longitude) as longitude,
        count(*) as geolocation_observation_count,
        count(latitude) as coordinate_observation_count
    from geolocation
    group by geo_zip_code_prefix

),

final as (

    select
        ranked_locations.geo_zip_code_prefix,
        ranked_locations.geo_city,
        ranked_locations.geo_state_code,
        coordinates.latitude,
        coordinates.longitude,
        coordinates.geolocation_observation_count,
        coordinates.coordinate_observation_count
    from ranked_locations
    inner join coordinates
        on ranked_locations.geo_zip_code_prefix
            = coordinates.geo_zip_code_prefix
    where ranked_locations.location_rank = 1

)

select *
from final