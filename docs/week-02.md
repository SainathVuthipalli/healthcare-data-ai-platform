# Week 2: Bronze, Silver, quarantine and idempotent MERGE

## What you are building

```
claims/*.csv --Auto Loader--> bronze_claims --rules--> silver_claims   (typed, de-duplicated, MERGEd)
                                                 \---> quarantine_claims (bad rows + every reason)
                                                  \--> silver_file_log   (counts per file)
manifest.json ----------------------------------------> 03_reconcile    (proves nothing was lost)
```

The rules live in `src/silver_rules.py` and are tested locally. The notebooks only add Databricks plumbing (Auto Loader, Delta MERGE).

## Run it in Databricks

1. **Get the code into Databricks.** In your workspace: Workspace, Create, **Git folder**, paste
   `https://github.com/SainathVuthipalli/healthcare-data-ai-platform` and create it.
   If the repo is private, link your GitHub account first (Settings, Linked accounts).
   *No Git folder available?* Make a plain workspace folder, import the three notebooks and the three
   `src/*.py` files into it, and the notebooks will find the modules next to themselves.
2. **Run `notebooks/01_bronze_ingest`** (Run all, serverless compute).
   Expect `claims_batch_1.csv = 9797` in the last table.
3. **Run `notebooks/02_silver_claims`.**
   Expect `silver_claims = 9118` and `quarantine_claims = 582`.
4. **Run `notebooks/03_reconcile`.**
   `manifest.json` must be at the top level of the volume. Expect every line `PASS` and
   "Reconciliation passed". Only batch 1 is in the log so far.
5. **Prove idempotency.** Run notebook 02 again. It should print "Nothing new to process" and the
   counts must not move. Take a screenshot.
6. **Add batch 2.** Upload `claims_batch_2.csv` into the `claims` folder of the volume. Then run 01, 02, 03 in order.

| After | bronze rows | silver_claims | quarantine_claims | inserted / updated / unchanged (batch 2) |
| --- | --- | --- | --- | --- |
| batch 1 | 9,797 | 9,118 | 582 | n/a |
| batch 2 | 20,592 | 18,776 | 1,224 | 9,658 / 194 / 194 |

7. **Re-run 02 and 03 once more.** Nothing changes and everything still passes.

These numbers come from running the same code on the same seed-42 files on a local Spark session.
If yours differ, the difference is the thing to investigate, so tell me which row and by how much.

## If something goes wrong

| Symptom | Likely cause and fix |
| --- | --- |
| `ModuleNotFoundError: platform_config` | The notebook could not find `src/`. Check the printed "code folder" line. Use a Git folder, or import the `src` files next to the notebooks. |
| Auto Loader or `cloudFiles` is not supported | Use the `COPY INTO` fallback at the bottom of notebook 01. Everything after Bronze is unchanged. |
| `FileNotFoundError` for the manifest | Upload `manifest.json` to the top level of the `landing` volume. |
| Permission or catalog errors | Confirm the catalog is `workspace`, the schema `healthcare`, the volume `landing`. Names are set in `src/platform_config.py`. |
| Silver count differs from the table above | Run notebook 03 and read the FAIL lines. They say which reason or total is off. |

## Local tests (optional on Windows)

`pytest` runs the 46 plain tests. The 14 Spark tests are skipped unless you `pip install pyspark`
and have Java 17 or newer, which is more setup than it is worth on Windows. They have already passed on Linux.

## Screenshots worth taking for the README

- Notebook 03 showing all PASS.
- Notebook 02 re-run showing "Nothing new to process".
- The `silver_file_log` table after both batches.

## Interview practice (90 seconds each, using this project)

- **How is the load idempotent?** MERGE on `claim_id`, update only when `updated_at` is strictly newer, so
  a replay or an unchanged re-send changes nothing. Quarantine is keyed by a row hash. Processed files are logged.
- **How do you handle late-arriving and corrected data?** A late claim is simply a new `claim_id`, so it inserts.
  A correction has the same `claim_id` and a newer `updated_at`, so it updates. Out-of-order older data cannot overwrite newer data.
- **Why quarantine instead of dropping bad rows?** You keep the evidence, every reason, and can fix upstream and replay.
  Dropping hides data loss; the reconciliation identity (in = quarantined + duplicates + unique valid) would not balance.
- **Why keep Bronze as strings?** One malformed value must not fail ingestion or lose the row. Typing is Silver's job.
- **Why `try_cast`?** Serverless compute runs with ANSI mode on, where a bad cast raises an error and kills the job.
  `try_cast` turns it into null, which a rule then catches.
- **How would you prove no rows were lost?** The accounting identity per file, plus table-level checks, against a
  ground-truth manifest. That is `03_reconcile`.

## Exercises

1. Add a rule `future_updated_at` (an `updated_at` later than the load time) to `silver_rules.py`, with a test.
2. Change the MERGE condition from `>` to `>=` and explain, using the unchanged re-sends, why that breaks idempotency in the logged counts.
3. Make processing a file atomic (the known limit at the bottom of notebook 02). Hint: write the log row in the same
   transaction, or derive "processed" from a column on the Silver rows instead of a separate log.

## Commit message

```
Add Bronze/Silver notebooks, validation rules, quarantine and reconciliation
```
