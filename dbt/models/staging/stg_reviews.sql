with source as (

    select *
    from {{ source('olist_raw', 'reviews') }}

),

renamed as (

    select
        review_id,
        order_id,
        score as review_score,
        title as review_title,
        message as review_message,
        creation_date as review_created_ts,
        answer_ts as review_answered_ts,
        cast(_batch_id as {{ dbt.type_string() }}) as ingestion_batch_id,
        cast(_ingested_at as {{ dbt.type_timestamp() }}) as source_ingested_at,
        cast(_record_hash as {{ dbt.type_string() }}) as source_record_hash
    from source

)

select *
from renamed
