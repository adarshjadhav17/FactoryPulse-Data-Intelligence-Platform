-- Stage generated sample CSVs for schema inference and raw-table loading.
-- Run these PUT commands from SnowSQL.
-- Replace /absolute/path/to/data_eng with your local repository path.

PUT file:///absolute/path/to/data_eng/data/sample/train_numeric_sample.csv
  @MANUFACTURING_DEFECTS.RAW.BOSCH_SAMPLE_STAGE
  AUTO_COMPRESS = TRUE
  OVERWRITE = TRUE;

PUT file:///absolute/path/to/data_eng/data/sample/train_date_sample.csv
  @MANUFACTURING_DEFECTS.RAW.BOSCH_SAMPLE_STAGE
  AUTO_COMPRESS = TRUE
  OVERWRITE = TRUE;

PUT file:///absolute/path/to/data_eng/data/sample/train_categorical_sample.csv
  @MANUFACTURING_DEFECTS.RAW.BOSCH_SAMPLE_STAGE
  AUTO_COMPRESS = TRUE
  OVERWRITE = TRUE;
