# Week 3: Silver reference tables and Gold

## What you are building

```
bronze_patients / providers / eligibility --04--> silver_patients / providers / eligibility
                                                              |
silver_claims (from Week 2) -----------------------------------+--05--> gold_claims_monthly
                                                                 \---> gold_provider_kpis
                                                                         |
                                            reconcile: Gold totals must equal Silver totals
```

The logic lives in `src/gold_rules.py` and is tested locally, including against an independent plain-Python
calculation. The notebooks only add Databricks plumbing (reading and saving tables).

## Run it in Databricks

1. **Pull the new code.** Open your Git folder `healthcare-data-ai-platform` in Workspace, click the branch name
   (`main`) near the top, and choose **Pull**. Button names may differ slightly. `notebooks/04_silver_reference`
   and `05_gold_build` should now appear.
2. **Run `04_silver_reference`** (Run all). Expect `patients 2000`, `providers 150`, `eligibility 2000`.
3. **Run `05_gold_build`.** Expect all six reconciliation lines `PASS` and the final line
   "Gold matches Silver: no claim lost or counted twice, and amounts agree to the cent."
4. **Prove it is repeatable.** Run 04 and 05 again. Every count and total must be identical.

| What | Expected |
| --- | --- |
| silver_patients / silver_providers / silver_eligibility | 2,000 / 150 / 2,000 |
| silver_claims | 18,776 |
| gold_claims_monthly | 18 rows, 18,776 claims, $3,873,229.27 billed |
| gold_provider_kpis | 150 rows (every provider has claims), 18,776 claims, $3,873,229.27 billed |
| claims outside coverage | 627 in total: 103 in January, 524 in February |
| denial rate across providers | between 0.0756 and 0.2406 |

These come from running the same code on the seed-42 files on a local Spark session, not in Databricks.
If a number differs, that is the thing to investigate: tell me which one and by how much.

## Why February has so many claims outside coverage

The generator ends coverage for about 10% of patients a few weeks into the data. January claims are mostly
inside coverage; many February claims fall after termination. In a real payer this is money paid for people who
were no longer covered, which is why the number is worth surfacing in Gold rather than hiding.

## If something goes wrong

| Symptom | Likely cause and fix |
| --- | --- |
| `ModuleNotFoundError: gold_rules` | The Git folder has not pulled. Pull again, then check the first cell's printed code folder. |
| `Table or view not found: silver_claims` | Weeks 1 and 2 notebooks have not been run. Run 01, 02 first. |
| A FAIL line in the reconciliation | Read which check failed. A claim-count mismatch points at the join in `enrich_claims`; an amount mismatch at the aggregates. |
| Counts differ between runs | They should not. Gold is overwritten from Silver. Check that nobody ran 02 in between. |

## Local tests

`pytest` runs the plain tests, including the six new Gold-versus-Silver reconciliation tests. The Spark tests
are skipped unless PySpark and Java are installed. They have already passed on Linux.

## Interview practice (90 seconds each, using this project)

- **Why overwrite Gold instead of MERGE?** Gold is a pure function of Silver, so a rebuild is always correct and has no
  state to drift. Go incremental only when a full rebuild is too slow, and then replace just the affected months.
- **How do you avoid double counting in a join?** The coverage flag uses a semi-join, which can never multiply rows, and a
  test with two overlapping eligibility periods proves it. The reconciliation then checks the row count end to end.
- **How do you prove Gold equals Silver?** Compare claim counts and total amounts to the cent, read back from the saved
  tables. Amounts use decimals, never floats, because floats drift.
- **Why does Gold contain no names or health card numbers?** Least privilege. Gold is the widest-read layer, so it
  should carry only what the analysis needs. The sensitive columns stay in `silver_patients`, where Week 4 puts masks.
- **What business insight does it show?** 627 claims were for services after coverage ended, 524 of them in February.
  That is a real financial-leakage signal, and it came from a data-quality rule on top of the pipeline.

## Exercises

1. Add `gold_specialty_summary` (claims, billed amount and denial rate by provider specialty) to `gold_rules.py`, with a
   test that recomputes it in plain Python the way `test_gold_matches_an_independent_python_calculation` does.
2. Change the coverage join in `enrich_claims` to an inner join. Run the tests and watch which check catches it.
   This is the point of reconciliation: the mistake is silent without it.
3. Rebuild `gold_claims_monthly` incrementally by replacing only the months touched by new Silver rows
   (look up `replaceWhere` for Delta).

## Commit message

```
Add Silver reference tables, Gold aggregates and Gold-vs-Silver reconciliation
```
