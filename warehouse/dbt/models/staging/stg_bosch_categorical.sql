with source as (
    select * from {{ source('raw_bosch', 'TRAIN_CATEGORICAL') }}
)

select
    ID as defect_id,
    L0_S1_F25 as station_1_category_25,
    L0_S1_F27 as station_1_category_27,
    L0_S1_F29 as station_1_category_29,
    L0_S1_F31 as station_1_category_31,
    L0_S2_F33 as station_2_category_33
from source
