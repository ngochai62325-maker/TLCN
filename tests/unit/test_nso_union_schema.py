"""All NSO chunks expose the union before Bronze creates its first table."""
from dataclasses import replace
import pandas as pd
from ingestion.adapters.nso_adapter import NsoVietnamAdapter
from ingestion.config.registry import SourceRegistry
from ingestion.storage.bronze_writer import BronzeIcebergWriter, sanitize_column_name


def test_national_first_chunk_contains_matrix_columns_and_preserves_footer(tmp_path):
    registry = SourceRegistry('config/ingestion/source_registry.yaml')
    registry.load()
    config = replace(registry.get_source('nso'), local_path='data/raw/nso')
    chunks = list(NsoVietnamAdapter().extract_full(config, str(tmp_path)))
    assert len(chunks) == 13
    assert sum(c.record_count for c in chunks) == 833
    names = [[sanitize_column_name(c) for c in chunk.data.columns] for chunk in chunks]
    assert all(n == names[0] for n in names)
    assert len(names[0]) == 42  # 41 raw source fields + per-file lineage.
    assert 'col_1995' in names[0] and 'so_b_2024' in names[0]
    matrix = chunks[1].data
    assert matrix['1995'].notna().sum() > 0
    assert pd.isna(chunks[0].data['1995']).all()
    writer = BronzeIcebergWriter()
    for chunk in chunks:
        for column in chunk.data.columns:
            assert writer._map_dtype_to_trino(chunk.data[column].dtype) == 'VARCHAR'
    assert chunks[0].data['1995'].dtype == pd.StringDtype()
    province_column = next(c for c in chunks[0].data if sanitize_column_name(c) == 't_nh_th_nh_ph_')
    assert chunks[0].data[province_column].dtype == pd.StringDtype()
    assert all(chunk.data.drop(columns=['_source_file']).isna().all(axis=1).sum() == 1 for chunk in chunks)
