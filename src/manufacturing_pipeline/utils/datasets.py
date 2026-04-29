"""Shared dataset metadata for local manufacturing pipeline steps."""

from __future__ import annotations


DATASETS = {
    "numeric": {
        "sample_path": "data/sample/train_numeric_sample.csv",
        "topic": "bosch.train.numeric",
        "expected_field_count": 970,
    },
    "date": {
        "sample_path": "data/sample/train_date_sample.csv",
        "topic": "bosch.train.date",
        "expected_field_count": 1157,
    },
    "categorical": {
        "sample_path": "data/sample/train_categorical_sample.csv",
        "topic": "bosch.train.categorical",
        "expected_field_count": 2141,
    },
}


def dataset_names() -> list[str]:
    return sorted(DATASETS)

