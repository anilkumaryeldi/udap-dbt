WITH cleaned_data AS
  (SELECT loan_id,
          CAST(payment_date AS DATE) AS payment_date,
          ROUND(expected_emi, 2) AS expected_emi,
          ROUND(actual_payment, 2) AS actual_payment
   FROM {{ source('raw_data', 'loan_payments') }})
SELECT *
FROM cleaned_data