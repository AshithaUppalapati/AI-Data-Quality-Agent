# Data Dictionary - orders

_Auto-generated 2026-07-29T12:44:10 from `data/schema/orders_schema.yml` and live Delta Lake metrics. Regenerate with `python src/llm_agent/documentation_generator.py` after a schema or pipeline change - do not hand-edit._

# Data Dictionary for `orders` Table

The `orders` table contains e-commerce order line items ingested from six synthetic CSV batches into the medallion pipeline's raw layer. Each row represents a unique order, and the schema has evolved over time to accommodate additional fields.

## order_id
- **Purpose**: Unique identifier for the order.
- **Data Type**: String
- **Notable Data Quality Behavior**: This column has a null rate of 0% across all batches, indicating consistent data quality.

## customer_id
- **Purpose**: Foreign key referencing the customer who placed the order.
- **Data Type**: String
- **Notable Data Quality Behavior**: The null rate ranges from 1.5764% in batch 6 to 5.0147% in batch 4, showing some variability in data quality across batches.

## order_date
- **Purpose**: Timestamp indicating when the order was placed.
- **Data Type**: Timestamp
- **Notable Data Quality Behavior**: The null rate fluctuates between 1.4706% in batch 2 and 4.8181% in batch 4, indicating some inconsistencies in data quality.

## product_id
- **Purpose**: Foreign key referencing the ordered product.
- **Data Type**: String
- **Notable Data Quality Behavior**: The null rate varies from 1.5549% in batch 1 to 6.588% in batch 4, suggesting occasional data quality issues.

## product_category
- **Purpose**: Product category label (e.g., Electronics, Clothing).
- **Data Type**: String
- **Notable Data Quality Behavior**: The null rate increases from 1.5549% in batch 1 to 6.3913% in batch 4, indicating a trend of declining data quality over time.

## quantity
- **Purpose**: Units ordered.
- **Data Type**: Integer
- **Notable Data Quality Behavior**: The null rate ranges from 1.6667% in batch 2 to 5.0147% in batch 4, showing some variability in data quality.

## unit_price
- **Purpose**: Price per unit, in USD.
- **Data Type**: Double
- **Notable Data Quality Behavior**: The null rate varies from 1.6667% in batch 2 to 5.0049% in batch 3, indicating some inconsistencies in data quality.

## total_amount
- **Purpose**: Computed value representing quantity multiplied by unit price.
- **Data Type**: Double
- **Notable Data Quality Behavior**: The null rate ranges from 2.3324% in batch 1 to 6.9813% in batch 4, suggesting variability in data quality.

## order_status
- **Purpose**: Lifecycle state of the order.
- **Data Type**: String
- **Notable Data Quality Behavior**: The null rate increases from 2.0588% in batch 2 to 7.3746% in batch 4, indicating a decline in data quality over time.

## payment_method
- **Purpose**: Payment instrument used for the order.
- **Data Type**: String
- **Notable Data Quality Behavior**: The null rate varies from 1.2634% in batch 1 to 5.8014% in batch 4, showing some inconsistencies in data quality.

## batch_num
- **Purpose**: Identifies which of the 6 synthetic ingestion batches produced this row.
- **Data Type**: Integer
- **Notable Data Quality Behavior**: This column has a null rate of 0% across all batches, indicating consistent data quality.

## state
- **Purpose**: Single combined US state field used before the shipping/billing split.
- **Data Type**: String
- **Notable Data Quality Behavior**: Present only in batches 1-2, with null rates of 1.7493% and 2.3529%, respectively. This column was replaced by `shipping_state` and `billing_state` starting in batch 3, reflecting expected schema evolution.

## shipping_state
- **Purpose**: US state code for the shipping address.
- **Data Type**: String
- **Notable Data Quality Behavior**: Introduced in batch 3, the null rate ranges from 4.318% in batch 3 to 2.5616% in batch 6, showing improving data quality over time.

## billing_state
- **Purpose**: US state code for the billing address.
- **Data Type**: String
- **Notable Data Quality Behavior**: Introduced in batch 3, the null rate ranges from 5.4956% in batch 3 to 2.4631% in batch 6, indicating a trend of improving data quality.

## discount_pct
- **Purpose**: Discount applied to the order, range 0.0-0.30.
- **Data Type**: Double
- **Notable Data Quality Behavior**: Introduced in batch 5, the null rate is 4.2885% in batch 5 and improves to 2.069% in batch 6, reflecting expected schema evolution.

# Summary of Schema Drift and Rule Violations
The `orders` table experienced schema evolution across the six batches, with the number of columns increasing from 12 in batches 1 and 2 to 14 by batch 5. The introduction of `shipping_state`, `billing_state`, and `discount_pct` reflects planned schema changes. Rule violations were prevalent in the earlier batches, with batch 4 showing the highest total of 239 violations, while batch 6 had no rule violations, indicating significant improvements in data quality.