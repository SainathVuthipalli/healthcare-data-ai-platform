# Healthcare Data and AI Platform

A lakehouse for synthetic healthcare claims, with a retrieval-augmented (RAG) assistant on top.
Built to practise and demonstrate production data engineering for AI: idempotent loads,
data quality, governance, evaluation and cost tracking.

**All data is synthetic.** No real patient, provider or employer data is used. Health card
numbers carry a `SYN-` prefix so they cannot be mistaken for real identifiers.

## Status

- [x] Week 1: seeded data generator with injected bad and late records (this commit)
- [ ] Week 2: Bronze (Auto Loader) and Silver (quarantine, idempotent MERGE)
- [ ] Week 3: Gold tables and reconciliation
- [ ] Week 4: Unity Catalog masks, row filters, lineage
- [ ] Weeks 5-6: MLflow model, embeddings, vector index
- [ ] Weeks 7-8: RAG service and evaluation
- [ ] Weeks 9-10: Docker, CI, monitoring, demo

## Quickstart

```bash
pip install -r requirements.txt
python src/generate_data.py --seed 42 --out data/landing
pytest
```

The generator uses only the Python standard library. The same seed always produces
byte-identical files.

## What it generates

| File | Rows (default) | Purpose |
| --- | --- | --- |
| `patients.csv` | 2,000 | Synthetic members with fake health card numbers |
| `providers.csv` | 150 | Synthetic clinics |
| `eligibility.csv` | 2,000 | Coverage periods, about 10% terminated |
| `claims_batch_1.csv` | 9,797 | First extract (service dates in days 1-28) |
| `claims_batch_2.csv` | 10,795 | Second extract: new claims plus late arrivals, corrections and re-sends |
| `manifest.json` | n/a | Ground truth: exactly what was injected |

### What batch 2 contains

- **New claims** for the second 28-day period.
- **Late-arriving claims:** period-1 service dates that were withheld from batch 1 (300 by default).
- **Corrections:** the same `claim_id` as batch 1 with a changed amount or status and a newer `updated_at` (194).
- **Unchanged re-sends:** exact copies of batch-1 rows (194). An idempotent load must not double-count these.

### Injected defects

Each defect type hits a distinct set of rows, so counts in `manifest.json` are exact.

| Defect | Example | Expected pipeline action |
| --- | --- | --- |
| `null_patient_id` | empty `patient_id` | reject to quarantine |
| `orphan_provider` | `PRX1234` not in providers | reject to quarantine |
| `negative_amount` | `billed_amount` below 0 | reject to quarantine |
| `invalid_dx_format` | `E1199` | reject to quarantine |
| `invalid_service_date` | `2025-02-30` | reject to quarantine |
| `service_after_submission` | service date after submit date | reject to quarantine |
| `status_case` | `paid` instead of `PAID` | standardize |
| `dup_in_file` | identical row twice in one file | deduplicate |

## Design decisions

- **Seeded and clock-free.** Reproducible runs make pipeline bugs reproducible too.
- **A manifest of ground truth.** Later layers can prove reconciliation: rows in, rows
  rejected by reason, rows loaded, with no unexplained difference.
- **Tests re-detect the defects independently.** `tests/test_generator.py` checks the
  manifest against separate validators, across four seeds. This caught two real
  generator bugs on day one (see `docs/day-01.md`).

## Layout

```
src/generate_data.py     generator and CLI
tests/test_generator.py  pytest suite
docs/day-01.md           notes, exercises, interview practice
data/                    generated files (git-ignored)
```
