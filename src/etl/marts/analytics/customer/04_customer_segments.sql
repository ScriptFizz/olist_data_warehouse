CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_segments` AS
SELECT
    cm.customer_unique_id,
    CASE
        WHEN cm.monetary_value < 100 THEN 'Low value'
        WHEN cm.monetary_value BETWEEN 100 AND 500 THEN 'Mid value'
        ELSE 'High value'
    END AS monerary_segment,
    CASE
      WHEN cm.monetary_value > 500 AND cb.avg_installments > 3 THEN 'High-ticket planner'
      WHEN cm.monetary_value < 100 AND cb.items_per_order = 1 THEN 'Impulse buyer'
      WHEN cb.freight_ratio > 0.3 THEN 'Freight-sensitive'
      WHEN cb.items_per_order >= 3 THEN 'Multi-item shopper'
      ELSE 'Standard buyer'
    END AS behavior_segment

FROM `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_metrics` cm
JOIN `{{ PROJECT_ID }}.{{ ANALYTICS_DATASET_ID }}.customer_behavior_features` cb
ON cm.customer_unique_id = cb.customer_unique_id;
