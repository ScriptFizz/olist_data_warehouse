import pandas as pd

from olist_dw.etl.load.ingestion_batch import (
    IngestionBatch,
    compute_batch_fingerprint,
    compute_record_hashes,
)


def example_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "identifier": ["a", "b"],
            "amount": [10.5, None],
            "occurred_at": pd.to_datetime(
                ["2024-01-01 10:00:00", "NaT"]
            ),
        }
    )


def test_record_hashes_are_deterministic() -> None:
    dataframe = example_table()

    first = compute_record_hashes(dataframe)
    second = compute_record_hashes(dataframe.copy(deep=True))

    assert first.tolist() == second.tolist()
    assert first.str.len().tolist() == [64, 64]


def test_changed_value_changes_record_hash() -> None:
    original = example_table()
    changed = example_table()
    changed.loc[0, "amount"] = 11.5

    original_hashes = compute_record_hashes(original)
    changed_hashes = compute_record_hashes(changed)

    assert original_hashes.iloc[0] != changed_hashes.iloc[0]
    assert original_hashes.iloc[1] == changed_hashes.iloc[1]


def test_default_hash_excludes_metadata_columns() -> None:
    original = example_table()
    with_metadata = original.assign(
        _ingestion_batch_id="different-every-run",
        _ingested_at="2026-09-01T10:00:00Z",
    )

    assert compute_record_hashes(original).tolist() == (
        compute_record_hashes(with_metadata).tolist()
    )


def test_batch_fingerprint_is_independent_of_row_and_table_order() -> None:
    first = {
        "orders": example_table(),
        "customers": pd.DataFrame(
            {"identifier": ["customer-1"]}
        ),
    }
    second = {
        "customers": pd.DataFrame(
            {"identifier": ["customer-1"]}
        ),
        "orders": example_table().iloc[::-1].reset_index(drop=True),
    }

    assert compute_batch_fingerprint(first) == (
        compute_batch_fingerprint(second)
    )


def test_batch_contains_counts_and_fingerprint() -> None:
    tables = {
        "first": example_table(),
        "second": pd.DataFrame({"identifier": ["x"]}),
    }

    batch = IngestionBatch.create(
        source_name="processed_olist",
        tables=tables,
    )

    assert batch.source_name == "processed_olist"
    assert batch.table_count == 2
    assert batch.input_row_count == 3
    assert len(batch.source_fingerprint) == 64
    assert batch.started_at.tzinfo is not None
