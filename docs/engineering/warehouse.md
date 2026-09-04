# dbt warehouse

dbt is the only analytical transformation system in the repository. The old
hand-ordered SQL runner was removed so dependency order, contracts, and tests
cannot drift between two implementations.

## Layers

### Sources and staging

The nine `olist_raw` sources describe the Python-published tables and their key
quality expectations. Staging views rename fields into analytical vocabulary,
apply portable casts, and expose ingestion lineage consistently.

Staging models stay close to one source each. Business aggregation belongs
downstream.

### Intermediate

Intermediate views provide reusable transformations:

- normalized geolocation at ZIP-code grain;
- translated product categories;
- item and payment totals at order grain;
- deterministic review selection;
- the canonical enriched order record.

This prevents facts and dimensions from independently reproducing joins or
deduplication rules.

### Marts

| Model | Grain | Purpose |
|---|---|---|
| `dim_customers` | one row per `customer_id` | Order-address customer attributes and geography |
| `dim_products` | one row per `product_id` | Product and translated category attributes |
| `dim_sellers` | one row per `seller_id` | Seller attributes and geography |
| `fct_orders` | one row per `order_id` | Lifecycle, delivery, payment, item, and review measures |
| `fct_order_items` | one row per `(order_id, order_item_id)` | Item and freight measures linked to dimensions |

Natural keys are used because this is a single-source warehouse with stable
source identifiers. Hash surrogate keys would add complexity without resolving
a real identity problem.

## Measures

Item price, freight, payment value, and record counts are additive at their
declared grains. Distinct-product counts, review scores, durations, flags, and
payment differences require context and must not be summed indiscriminately.
Descriptions in the model YAML make these semantics visible to consumers.

## Incremental facts

`fct_orders` merges on `order_id`; `fct_order_items` merges on its composite
order-line key. `warehouse_updated_at` represents the newest ingestion time of
any source record capable of changing the fact row.

This permits a newly ingested correction to an old order to update the fact.
It is deliberately not based on purchase date. The intermediate layer remains
view-based, so the main optimization is fewer fact writes rather than a claim
that every upstream query scans only new raw rows.

## Contracts and tests

Model contracts enforce published column names and types. Generic tests cover
nullability, uniqueness, accepted values, and relationships. Singular tests
cover grain preservation, reconciliation, delivery semantics, flags, and
watermark invariants. dbt unit tests exercise incremental selection without
requiring production data.
