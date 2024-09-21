with customer_loans as (
    select
        c.customer_id,
        c.name as customer_name,
        c.address,
        c.email,
        c.phone,
        c.satisfaction_score,
        count(l.loan_id) as total_loans, -- Simple KPI: Total number of loans for each customer
        avg(l.loan_amount) as avg_loan_amount -- Simple KPI: Average loan amount per customer
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_loan_details') }} l on c.customer_id = l.customer_id
    group by
        c.customer_id, c.name, c.address, c.email, c.phone, c.satisfaction_score
)

select * from customer_loans
