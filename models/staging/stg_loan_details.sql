{{ config(
    materialized='view'
) }}
WITH cleaned_data AS (
    SELECT
        loan_id,
        customer_id,
        ROUND(loan_amount, 2) AS loan_amount,
        loan_type,
        CAST(origination_date AS DATE) AS origination_date,
        status,
        ROUND(ltv_ratio, 2) AS ltv_ratio,
        credit_score,
        geographic_location,
        ROUND(dti_ratio, 2) AS dti_ratio,
        delinquency_days,
        ROUND(servicing_cost, 2) AS servicing_cost,
        ROUND(total_payments_collected, 2) AS total_payments_collected,
        ROUND(total_amount_delinquent, 2) AS total_amount_delinquent,
        ROUND(emi_amount, 2) AS emi_amount,
        loan_tenure_years
    FROM {{ source('raw_data', 'loan_details') }}
)
SELECT * FROM cleaned_data
