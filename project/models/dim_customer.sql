with stg_customers as (
    select * from {{ source('fudgemart_V3','fm_customers')}}
),
stg_accounts as (
    select * from {{ source('fudgeflix_V3','ff_accounts')}} as a
    join {{ source('fudgeflix_V3','ff_zipcodes')}} as z on a.account_zipcode = z.zip_code
)
select  {{ dbt_utils.generate_surrogate_key(['c.customer_id','a.account_id']) }} as customer_key, 
    c.customer_id, 
    a.account_id,
    COALESCE(c.customer_email, a.account_email) as customer_email,
    COALESCE(c.customer_firstname, a.account_firstname) as customer_firstname,
    COALESCE(c.customer_lastname, a.account_lastname) as customer_lastname,
    COALESCE(c.customer_address, a.account_address) as customer_address,
    COALESCE(c.customer_city, a.zip_city) as customer_city,
    COALESCE(c.customer_state, a.zip_state) as customer_state,
    COALESCE(c.customer_zip, a.account_zipcode) as customer_zip,
    c.customer_phone,
    c.customer_fax,
    a.account_plan_id,
    a.account_opened_on
from stg_customers c
full join stg_accounts a on c.customer_id = (a.account_id-10000)
