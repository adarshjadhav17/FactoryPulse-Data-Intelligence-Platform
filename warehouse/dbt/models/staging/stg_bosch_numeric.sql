with source as (
    select * from {{ source('raw_bosch', 'TRAIN_NUMERIC') }}
)

select
    ID as defect_id,
    RESPONSE as defect_response,
    L0_S0_F0 as station_0_feature_0,
    L0_S0_F2 as station_0_feature_2,
    L0_S0_F4 as station_0_feature_4,
    L0_S0_F6 as station_0_feature_6,
    L0_S0_F8 as station_0_feature_8
from source
