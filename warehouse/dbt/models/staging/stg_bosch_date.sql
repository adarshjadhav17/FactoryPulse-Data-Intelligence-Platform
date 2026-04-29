with source as (
    select * from {{ source('raw_bosch', 'TRAIN_DATE') }}
)

select
    ID as defect_id,
    L0_S0_D1 as station_0_date_1,
    L0_S0_D3 as station_0_date_3,
    L0_S0_D5 as station_0_date_5,
    L0_S0_D7 as station_0_date_7,
    L0_S0_D9 as station_0_date_9
from source
