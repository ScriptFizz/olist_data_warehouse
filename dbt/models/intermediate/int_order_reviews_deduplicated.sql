-- Grain: at most one selected review per order.
-- The ordering is deterministic:
-- 1. non-null answer timestamp;
-- 2. latest answer timestamp;
-- 3. non-null creation timestamp;
-- 4. latest creation timestamp;
-- 5. greatest review_id as a final stable tie-breaker.

with reviews as (

    select *
    from {{ ref('stg_reviews') }}

),

ranked as (

    select
        review_id,
        order_id,
        review_score,
        review_title,
        review_message,
        review_created_ts,
        review_answered_ts,

        count(*) over (
            partition by order_id
        ) as review_record_count,

        row_number() over (
            partition by order_id
            order by
                case when review_answered_ts is null then 1 else 0 end,
                review_answered_ts desc,
                case when review_created_ts is null then 1 else 0 end,
                review_created_ts desc,
                review_id desc
        ) as review_recency_rank
    from reviews

),

latest_review as (

    select
        review_id,
        order_id,
        review_score,
        review_title,
        review_message,
        review_created_ts,
        review_answered_ts,
        review_record_count
    from ranked
    where review_recency_rank = 1

)

select *
from latest_review