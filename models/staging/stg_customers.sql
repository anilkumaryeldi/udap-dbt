WITH cleaned_data AS (
    SELECT
        customer_id,
        name,
        address,
        email,
        phone,
        satisfaction_score
    FROM {{ source('raw_data', 'customer_info') }}
)
SELECT * FROM cleaned_data
