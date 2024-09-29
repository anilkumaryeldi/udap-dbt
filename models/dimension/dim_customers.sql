WITH customer_loans AS
  (SELECT c.customer_id,
          c.name AS customer_name,
          c.address,
          c.email,
          c.phone,
          c.satisfaction_score,
          count(l.loan_id) AS total_loans, -- Simple KPI: Total number of loans for each customer
 avg(l.loan_amount) AS avg_loan_amount -- Simple KPI: Average loan amount per customer

   FROM {{ ref('stg_customers') }} c
   LEFT JOIN {{ ref('stg_loan_details') }} l ON c.customer_id = l.customer_id
   GROUP BY c.customer_id,
            c.name,
            c.address,
            c.email,
            c.phone,
            c.satisfaction_score)
SELECT *
FROM customer_loans