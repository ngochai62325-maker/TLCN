"""NSO V06 observations with explicit source inventories and approval boundaries."""
import json
from pathlib import Path
from functools import reduce
from pyspark.sql import functions as F
from silver.engine.quality import SilverQualityEngine
from silver.mappings.nso_geography import apply_historical_geography
from silver.transformers.faostat_source import sql_rule

NATIONAL_MEASURES = {
    't_ng_di_n_t_ch_ngh_n_ha_': ('area', 'annual', '1000 hectare', 'hectare'),
    'di_n_t_ch_l_a_ng_xu_n_ngh_n_ha_': ('area', 'winter_spring', '1000 hectare', 'hectare'),
    'di_n_t_ch_l_a_h_thu_ngh_n_ha_': ('area', 'summer_autumn', '1000 hectare', 'hectare'),
    'di_n_t_ch_l_a_m_a_ngh_n_ha_': ('area', 'mua', '1000 hectare', 'hectare'),
    't_ng_s_n_lu_ng_ngh_n_t_n_': ('production', 'annual', '1000 tonne', 'tonne'),
    's_n_lu_ng_l_a_ng_xu_n_ngh_n_t_n_': ('production', 'winter_spring', '1000 tonne', 'tonne'),
    's_n_lu_ng_l_a_h_thu_ngh_n_t_n_': ('production', 'summer_autumn', '1000 tonne', 'tonne'),
    's_n_lu_ng_l_a_m_a_ngh_n_t_n_': ('production', 'mua', '1000 tonne', 'tonne'),
}
MATRIX_FILES = {
    f'V06.{n}.csv': (measure, season)
    for start, season in [(13, 'annual'), (16, 'winter_spring'),
                          (19, 'summer_autumn_autumn_winter'), (22, 'mua')]
    for n, measure in zip(range(start, start + 3), ('area', 'yield', 'production'))
}
NATIONAL_LABELS = ('C? NU?C', 'Cáº¢ NÆ¯á»šC', 'Cáº£ nÆ°á»›c')
COUNTRY_REFERENCE = 'https://unstats.un.org/unsd/methodology/m49/'
CONFIG = Path(__file__).resolve().parents[3] / 'config' / 'silver'


class NsoVietnamTransformer:
    dataset_id = 'nso_vietnam'
    business_keys = ['source_table', 'geography_name_raw', 'geography_level',
                     'year', 'season', 'measure', 'statistic_kind']

    def __init__(self, source_inventory=None, geography_entries=None):
        self.inventory = (json.loads((CONFIG / 'nso_source_tables.json').read_text(encoding='utf-8'))
                          if source_inventory is None else source_inventory)
        self.geographies = (json.loads((CONFIG / 'nso_geography_reviewed.json').read_text(encoding='utf-8'))
                            if geography_entries is None else geography_entries)
        self.candidates = json.loads((CONFIG / 'nso_geography_candidates.json').read_text(encoding='utf-8'))

    def _source_records(self, df):
        if '_source_file' not in df.columns:
            raise ValueError('NSO requires per-file _source_file lineage')
        df = df.withColumn('source_table', F.regexp_extract('_source_file', r'(?:^|/)(V06\.[0-9]+\.csv)$', 1))
        files = [r[0] for r in df.select('source_table').distinct().collect()]
        unknown = set(files) - set(self.inventory)
        if unknown:
            raise ValueError(f'NSO contains unprofiled source files: {sorted(unknown)}')
        empty, width = F.lit(False), F.lit(0)
        for file in files:
            source = self.inventory[file]
            missing = set(source['source_columns']) - set(df.columns)
            if missing:
                raise ValueError(f'NSO {file} missing source columns: {sorted(missing)}; repair Bronze schema')
            all_null = reduce(lambda a, b: a & b, [F.col(c).isNull() for c in source['source_columns']])
            empty = empty | ((F.col('source_table') == file) & all_null)
            width = F.when(F.col('source_table') == file, 8 if file == 'V06.12.csv'
                           else len(source['measure_columns'])).otherwise(width)
        if '_source_payload' not in df.columns:
            df = df.withColumn('_source_payload', F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)
                if c not in ('source_table', '_bronze_iceberg_snapshot_id')]), {'ignoreNullFields': 'false'}))
        return df.withColumn('_all_source_null', empty).withColumn('_candidate_observation_count', width), files

    def excluded_records(self, df):
        records, _ = self._source_records(df)
        return (records.filter('_all_source_null').withColumn('_exclusion_reason', F.lit('ALL_SOURCE_FIELDS_NULL'))
            .withColumn('_exclusion_record_id', F.sha2(F.concat_ws('|', 'source_table', '_source_payload'), 256))
            .withColumn('dq_errors', F.array(F.struct(F.lit('NSO_EMPTY_SOURCE_RECORD').alias('rule_id'),
                F.lit('All source fields NULL; no observations generated').alias('error_message'),
                F.lit('source_record').alias('failed_column')))))

    def observation_audit(self, df):
        records, _ = self._source_records(df)
        summary = records.agg(F.sum('_candidate_observation_count').alias('candidate'),
            F.sum(F.when(F.col('_all_source_null'), F.col('_candidate_observation_count')).otherwise(0)).alias('excluded')).first()
        exclusions = self.excluded_records(df).select('source_table', '_source_file', '_source_checksum',
            '_exclusion_record_id', '_exclusion_reason', '_candidate_observation_count').collect()
        return {'observation_count': summary.candidate or 0, 'excluded_count': summary.excluded or 0,
                'excluded_record_count': len(exclusions), 'excluded_records': [r.asDict() for r in exclusions]}

    def preprocess(self, df):
        records, files = self._source_records(df)
        if not files:
            # Build an empty result schema from a national exemplar; no row escapes.
            exemplar = df.sparkSession.createDataFrame([('V06.12.csv',)], '_source_file string')
            types = {c: df.schema[c].dataType for c in df.columns}
            for c in set(df.columns) | set(self.inventory['V06.12.csv']['source_columns']):
                if c != '_source_file':
                    exemplar = exemplar.withColumn(c, F.lit(None).cast(types.get(c, 'string')))
            return self.preprocess(exemplar).limit(0)
        records = records.filter('NOT _all_source_null')
        audit = [c for c in records.columns if c.startswith('_') and c not in ('_all_source_null', '_candidate_observation_count')]
        pieces = []
        if 'V06.12.csv' in files:
            entries = [F.struct(F.lit(c).alias('source_column'), F.col(c).cast('string').alias('value_raw'),
                F.lit(m).alias('measure'), F.lit(s).alias('season'), F.lit(u).alias('proposed_source_unit'),
                F.lit(v).alias('proposed_unit'), F.lit(1000).alias('proposed_factor'),
                F.lit('VERIFIED').alias('unit_status'), F.lit('V06.12 raw column header').alias('unit_source_reference'))
                for c, (m, s, u, v) in NATIONAL_MEASURES.items()]
            national = records.filter(F.col('source_table') == 'V06.12.csv').withColumn('o', F.explode(F.array(*entries)))
            pieces.append(national.select(*audit, 'source_table', F.lit('C? NU?C').alias('geography_name_raw'),
                F.col('nam').alias('period_raw'), F.col('gi_tr_v_ch_s_ph_t_tri_n').alias('statistic_raw'), 'o.*'))
        matrix_files = [f for f in files if f != 'V06.12.csv']
        if matrix_files:
            expression = None
            for file in matrix_files:
                source = self.inventory[file]
                if not source['measure_columns']:
                    raise ValueError(f'NSO {file}: no profiled measure columns')
                entries = [F.struct(F.lit(c).alias('source_column'), F.col(c).cast('string').alias('value_raw'),
                    F.lit(source['measure']).alias('measure'), F.lit(source['season']).alias('season'),
                    F.lit(source['proposed_source_unit']).alias('proposed_source_unit'),
                    F.lit(source['proposed_unit']).alias('proposed_unit'), F.lit(source['proposed_factor']).cast('int').alias('proposed_factor'),
                    F.lit(source['status']).alias('unit_status'), F.lit(source['source_reference']).alias('unit_source_reference'))
                    for c in source['measure_columns']]
                condition = F.col('source_table') == file
                expression = F.when(condition, F.array(*entries)) if expression is None else expression.when(condition, F.array(*entries))
            matrix = records.filter(F.col('source_table').isin(*matrix_files)).withColumn('o', F.explode(expression))
            pieces.append(matrix.select(*audit, 'source_table', F.col('t_nh_th_nh_ph_').alias('geography_name_raw'),
                F.lit('quantity').alias('statistic_raw'), 'o.*').withColumn('period_raw', F.col('source_column')))
        long = reduce(lambda a, b: a.unionByName(b), pieces)
        raw = F.trim(F.col('value_raw'))
        index = F.col('statistic_raw').isin('Ch? s? phát tri?n (Nam tru?c =100) - %',
                                         'Ch? s? phÃ¡t tri?n (Nam tru?c =100) - %')
        quantity = F.col('statistic_raw').isin('Giá tr?', 'GiÃ¡ tr?', 'GiÃ¡ trá»‹', 'quantity')
        long = (long.withColumn('year', F.expr("try_cast(regexp_extract(period_raw, '([0-9]{4})$', 1) as int)"))
            .withColumn('_period_valid', F.col('period_raw').rlike(r'^(?:[0-9]{4}|So b\? [0-9]{4}|SÆ¡ bá»™ [0-9]{4}|col_[0-9]{4}|so_b_[0-9]{4})$'))
            .withColumn('is_provisional', F.col('period_raw').rlike(r'^(?:so_b_|So b\?|SÆ¡ bá»™ )'))
            .withColumn('statistic_kind', F.when(quantity, 'quantity').when(index, 'development_index'))
            .withColumn('_missing_value', raw.isNull() | raw.isin('', '..'))
            .withColumn('missing_kind', F.when(raw.isNull(), 'source_null').when(raw == '..', 'source_marker')
                .when(raw == '', 'source_blank').otherwise('present'))
            .withColumn('value_source', F.when(~F.col('_missing_value'), F.expr('try_cast(value_raw as decimal(28,8))'))))
        long = (long.withColumn('proposed_source_unit', F.when(index, 'percent').otherwise(F.col('proposed_source_unit')))
            .withColumn('proposed_unit', F.when(index, 'percent').otherwise(F.col('proposed_unit')))
            .withColumn('proposed_factor', F.when(index, F.lit(1)).otherwise(F.col('proposed_factor'))))
        approved = F.col('unit_status').isin('VERIFIED', 'APPROVED') & (F.col('statistic_kind') == 'quantity')
        national_geo = F.col('geography_name_raw').isin(*NATIONAL_LABELS)
        regions = [label for label, (_, level) in self.candidates.items() if level == 'region']
        long = (long.withColumn('source_unit', F.when(index, 'percent').when(approved, F.col('proposed_source_unit')))
            .withColumn('unit', F.when(index, 'percent').when(approved, F.col('proposed_unit')))
            .withColumn('conversion_factor', F.when(index, F.lit(1)).when(approved, F.col('proposed_factor')).cast('decimal(18,6)'))
            .withColumn('value', (F.col('value_source') * F.col('conversion_factor')).cast('decimal(28,8)'))
            .withColumn('geography_level', F.when(national_geo, 'national').when(F.col('geography_name_raw').isin(*regions), 'region').otherwise('unknown'))
            .withColumn('geography_name', F.when(national_geo, 'Việt Nam'))
            .withColumn('geography_code', F.when(national_geo, '704'))
            .withColumn('geography_code_scheme', F.when(national_geo, 'UN_M49'))
            .withColumn('geography_source_reference', F.when(national_geo, COUNTRY_REFERENCE))
            .withColumn('geography_valid_from', F.lit(None).cast('date'))
            .withColumn('geography_valid_to', F.lit(None).cast('date'))
            .withColumn('parent_geography', F.lit(None).cast('string'))
            .withColumn('mapping_status', F.when(national_geo, 'verified').otherwise('needs_review'))
            .withColumn('country_code', F.lit('704')).withColumn('commodity_name', F.lit('Rice'))
            .withColumn('province_code', F.lit(None).cast('string'))
            .withColumn('_column_present', F.lit(True)).withColumn('gold_ready', F.lit(False)))
        if '_bronze_iceberg_snapshot_id' not in long.columns:
            long = long.withColumn('_bronze_iceberg_snapshot_id', F.lit(None).cast('long'))
        return apply_historical_geography(long, self.geographies)

    transform = preprocess

    def quality_rules(self):
        return [
            sql_rule('NSO_DATE', '_period_valid AND year BETWEEN 1960 AND year(current_date()) + 1', 'year', 'Invalid reference year'),
            sql_rule('NSO_GEOGRAPHY', "mapping_status = 'verified'", 'geography_name_raw', 'Historical geography needs exact dated mapping'),
            sql_rule('NSO_UNIT', 'unit IS NOT NULL AND conversion_factor > 0 AND statistic_kind IS NOT NULL', 'unit', 'Source series unit/statistic kind needs approval'),
            sql_rule('NSO_VALUE', '(_missing_value OR value_source IS NOT NULL) AND (value_source IS NULL OR value_source >= 0) AND (_missing_value OR conversion_factor IS NULL OR value IS NOT NULL)', 'value_source', 'Malformed, negative or normalized decimal overflow; missing stays NULL'),
        ]

    def validate(self, df):
        return SilverQualityEngine(self.quality_rules()).apply_rules(df)
