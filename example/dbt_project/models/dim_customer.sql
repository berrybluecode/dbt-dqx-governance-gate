select
  customer_id,
  customer_name,
  lower(email) as email,
  cast(age as int) as age,
  country_code,
  current_timestamp() as dbt_loaded_at
from {{ ref('raw_customers') }}
where scenario = '{{ var("scenario", "pass") }}'
