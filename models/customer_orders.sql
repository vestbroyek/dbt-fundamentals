-- Import CTEs
with

orders as (

    select

        id as order_id,
        user_id	as customer_id,
        order_date as order_placed_at,
        status as order_status,

    from 
        {{ ref('jaffle_shop', 'orders') }}

),

customers as (

    select * from {{ ref('jaffle_shop', 'customers') }}

),

payments as (

    select * from {{ ref('stripe', 'payment') }}

),

-- Intermediate CTEs
customer_orders as (

    select

        id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders -- fix?

    from 
        customers

    left join orders
    on customers.customer_id = orders.user_id

    group by 1
        
),

total_paid as (

    select 

        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid

    from payments

    where status <> 'fail'
    group by 1

),

paid_orders as (

    select

        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status

    from orders

    left join total_paid
    on order_id = total_paid.order_id

    left join customers
    on user_id = customers.id

),

-- Happy with paid_orders
-- Happy wtih customer_orders
-- Have the below left to do


select

    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    case    
        when customer_orders.first_order_date = paid_orders.order_placed_at
        then 'new'
        else 'return' 
    end as nvsr,

    x.clv_bad as customer_lifetime_value,
    customer_orders.first_order_date as fdos

from 
    {{ ref('paid_orders') }} as paid_orders

left join customer_orders using (customer_id)

left outer join 
(
    select

        paid_orders.order_id,
        sum(t2.total_amount_paid) as clv_bad

    from paid_orders

    left join paid_orders t2 
        on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id

    group by 1

    order by paid_orders.order_id

) as x on x.order_id = paid_orders.order_id

order by order_id



