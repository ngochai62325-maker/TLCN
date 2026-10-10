# NSO units and crop seasons ? E1

Status: exact source identities and season distinctions implemented; matrix unit applicability **NEEDS_APPROVAL**. No matrix conversion is activated by the proposal.

## Official evidence and limits

[NSO Statistical Yearbook 2018](https://www.nso.gov.vn/wp-content/uploads/2019/10/Nien-giam-2018-1.pdf) was downloaded and hashed: `e491e8a47a3119f3ad8d8fca935426aad3767cd1ded5e7ea785930385374d3e1`. PDF has 1,025 pages. Evidence `nso_official_unit_evidence.json` contains page positions and unit presence, without copying tables.

| Series grouping | Area printed pages | Yield printed pages | Production printed pages |
|---|---|---|---|
| Annual | 509?510 | 511?512 | 513?514 |
| Winter spring | 515?516 | 517?518 | 519?520 |
| Summer autumn plus autumn winter | 521 | 522 | 523 |
| Mua | 524?525 | 526?527 | 528?529 |

These official tables corroborate thousand hectares for area, quintals per hectare for yield, and thousand tonnes for production. Factors follow the unit definitions: 1000 ha ? ha ?1000; one quintal is 100 kg, so quintal/ha ? kg/ha ?100; 1000 t ? tonne ?1000. This corroboration does not certify that the unlabeled V06 matrix files covering 1995?2024 inherit the same units without exceptions. Raw matrix headers and metadata.txt contain no unit declaration, so series/period applicability is held for owner approval or an exact export source reference. No value magnitude is used as evidence for an automatic unit choice.

V06.12 raw headers directly specify thousand ha / thousand tonnes and activate ?1000 for quantity. Its development-index labels specify previous year=100 and percent: preserve ?1, never a quantity conversion. Index proposed and canonical unit fields are percent, factor 1. No development index is inferred for matrix files.

## Exact per-source proposal

| Source table | Measure | Season | Source ? canonical unit | Factor | Applicability |
|---|---|---|---|---|---|
| V06.13.csv | area | annual | 1000 hectare ? hectare | ?1000 | NEEDS_APPROVAL |
| V06.14.csv | yield | annual | quintal/hectare ? kg/hectare | ?100 | NEEDS_APPROVAL |
| V06.15.csv | production | annual | 1000 tonne ? tonne | ?1000 | NEEDS_APPROVAL |
| V06.16.csv | area | winter_spring | 1000 hectare ? hectare | ?1000 | NEEDS_APPROVAL |
| V06.17.csv | yield | winter_spring | quintal/hectare ? kg/hectare | ?100 | NEEDS_APPROVAL |
| V06.18.csv | production | winter_spring | 1000 tonne ? tonne | ?1000 | NEEDS_APPROVAL |
| V06.19.csv | area | summer_autumn_autumn_winter | 1000 hectare ? hectare | ?1000 | NEEDS_APPROVAL |
| V06.20.csv | yield | summer_autumn_autumn_winter | quintal/hectare ? kg/hectare | ?100 | NEEDS_APPROVAL |
| V06.21.csv | production | summer_autumn_autumn_winter | 1000 tonne ? tonne | ?1000 | NEEDS_APPROVAL |
| V06.22.csv | area | mua | 1000 hectare ? hectare | ?1000 | NEEDS_APPROVAL |
| V06.23.csv | yield | mua | quintal/hectare ? kg/hectare | ?100 | NEEDS_APPROVAL |
| V06.24.csv | production | mua | 1000 tonne ? tonne | ?1000 | NEEDS_APPROVAL |

V06.12 supplies annual, winter_spring, summer_autumn and mua, each for area and production. V06.19?21 identify the combined summer_autumn_autumn_winter series in local metadata.txt; it remains distinct from V06.12 summer_autumn. The metadata titles are stored verbatim in `nso_e1_bronze_profile.json` and source identities in `config/silver/nso_source_tables.json`. No season is renamed to annual, no annual value is decomposed, no combined season is split, and no totals are added across geography levels/seasons.

## Review choices

Recommended: approve the three proposed unit factors per exact matrix source and documented year range after checking the original export/unit reference. Alternative: retain all unresolved matrix series in quarantine with raw and parsed source values; keep normalized unit/factor/value NULL. Approve the five distinct season labels and prohibit aggregation across overlapping totals. The implementation and isolated tests support both verified conversions and conservative unresolved handling.
