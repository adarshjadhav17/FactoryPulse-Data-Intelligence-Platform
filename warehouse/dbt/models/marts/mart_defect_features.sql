with numeric_features as (
    select * from {{ ref('stg_bosch_numeric') }}
),

date_features as (
    select * from {{ ref('stg_bosch_date') }}
),

categorical_features as (
    select * from {{ ref('stg_bosch_categorical') }}
)

select
    numeric_features.defect_id,
    numeric_features.defect_response,
    numeric_features.station_0_feature_0,
    numeric_features.station_0_feature_2,
    numeric_features.station_0_feature_4,
    numeric_features.station_0_feature_6,
    numeric_features.station_0_feature_8,
    date_features.station_0_date_1,
    date_features.station_0_date_3,
    date_features.station_0_date_5,
    date_features.station_0_date_7,
    date_features.station_0_date_9,
    categorical_features.station_1_category_25,
    categorical_features.station_1_category_27,
    categorical_features.station_1_category_29,
    categorical_features.station_1_category_31,
    categorical_features.station_2_category_33
from numeric_features
inner join date_features
    on numeric_features.defect_id = date_features.defect_id
inner join categorical_features
    on numeric_features.defect_id = categorical_features.defect_id
