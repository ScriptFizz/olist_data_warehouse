-- Monetary and behavior segments data for each customer
-- Grain: 1 row per (snapshot_date, customer_unique_id)

CREATE OR REPLACE VIEW `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.sem_customer_segment` AS
SELECT
    kc.snapshot_date,
    kc.customer_unique_id,
    CASE
    
        WHEN kc.monetary_value < 100 THEN 'Low value'
        WHEN kc.monetary_value < 500 THEN 'Mid value'
        ELSE 'High value'
    END AS monetary_segment,
    
    CASE
      
      WHEN kc.frequency = 0 
          THEN 'New / inactive'
      
      WHEN kc.monetary_value < 100 AND kc.items_per_order = 1 
          THEN 'Impulse buyer'
          
      WHEN kc.monetary_value > 500 AND kc.avg_installments > 3 
          THEN 'High-ticket planner'
      
      WHEN kc.items_per_order >= 3 
          THEN 'Multi-item shopper'
      
      ELSE 'Standard buyer'
    END AS behavior_segment,
    
    CASE
    
      WHEN kc.freight_ratio >= 0.3
          THEN 'Freight-sensitive'
      
      ELSE 'Not freight-sensitive'
    END AS freight_segment

FROM `{{ PROJECT_ID }}.{{ KPI_DATASET_ID }}.kpi_customer` kc;


