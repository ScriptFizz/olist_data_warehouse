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
        answer_ts as review_answered_ts
    from source

)

select *
from renamed