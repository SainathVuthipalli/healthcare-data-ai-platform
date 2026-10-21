# Week 4: governance, lineage and resume v1

## What you are building

```
governance_roles (who is who) --read by--> mask_name / mask_health_card / mask_dob   --SET MASK-->  silver_patients
                                      \--> plan_row_filter                          --SET ROW FILTER-> silver_patients
steward: real values, all rows | analyst: masked values, own plan only | no role: masked, no rows
```

The logic is in `src/governance_sql.py` and was tested on a local Spark session with 24 tests. Attaching the
policies (`ALTER TABLE ... SET MASK`) needs Unity Catalog, so notebook 06 is where that part is proven for real.
I have not run it in Databricks, so the first run is the test.

## Run it in Databricks

1. **Pull the new code** in your Git folder (branch name, then **Pull**).
2. **Run `06_governance`** from the top, cell by cell for the first run.
   - After section 1 you should see `is_steward() -> True`. If that cell fails, stop and send me the exact error.
   - Section 2 prints five `ok:` lines (four masks and one row filter).
   - Section 3 prints a `[PASS]` line for each check, then "Governance works: each role sees exactly what it should."
3. **Take the screenshots** in section 5: the analyst view (masked), then switch back and screenshot the steward view.
4. **Run `05_gold_build` again** as a steward. Its new guard passes, and the Gold numbers are unchanged.

| Check | Expected (seed-42 data) |
| --- | --- |
| steward sees | 2,000 patients, real names |
| analyst limited to PLAN-A sees | 658 patients, `***` names, only PLAN-A |
| analyst on ALL plans sees | 2,000 patients, still masked |
| health card for an analyst | like `********5678` (last 4 digits only) |
| date of birth for an analyst | 1 January of the real birth year |
| a user with no role sees | 0 rows |

## Lineage screenshot

In Catalog, open `workspace.healthcare.gold_provider_kpis` and click the **Lineage** tab. You want the graph that
runs from the Bronze tables through Silver to Gold. Take the same screenshot for `gold_claims_monthly`.
If the tab is empty, tell me what you see; lineage can take a few minutes to appear after a run.

## If something goes wrong

| Symptom | Likely cause and fix |
| --- | --- |
| Section 1 fails creating a function | Unity Catalog rejected a lookup table inside a function. Send me the exact error; the fallback is to compare `SESSION_USER()` with your email inside each function. |
| `SET MASK` or `SET ROW FILTER` fails | Send me the error text. Possible causes are a permission on the table or a feature limit on Free Edition. |
| Notebook 05 says "You can see N of 2000 patients" | You are not a steward. Run the last cells of 06 (or `set_role("steward")`) and try again. |
| A notebook cannot read `silver_patients` | Your role was left as analyst or removed. Run section 1 of 06 again, which restores steward. |
| Need to remove everything | Set `RUN_UNDO = True` in 06 and run that cell. It drops the policies first. |

## Design choices worth being able to explain

- **Default deny.** Someone missing from the roles table sees masked values and no rows, so forgetting to add a person never exposes data.
- **Policies on the table, not views.** A mask or filter applies to every way of reading the table. A view can be bypassed by querying the table directly.
- **A pipeline identity.** Notebooks 04 and 05 must run as a steward, or a row filter silently hides patients and Gold is wrong. Notebook 05 stops instead.
- **One row filter per table.** Databricks allows a single row filter on a table, so any new rule has to be combined into the same function.
- **A roles table instead of groups.** Free Edition has no account console. In production, use account groups with `IS_ACCOUNT_GROUP_MEMBER`, which also avoids a table lookup on every query.
- **Gold has no identifiers.** Analysts can use Gold freely, so the sensitive columns never leave `silver_patients`.

## Resume v1 (the gate: start applying after this)

Put the project right after your summary, labelled as a personal project on synthetic data. Replace every
bracket with a number you saw in your own output before you use it. The test count is what your own `pytest`
prints (64 passed on a setup without PySpark; more where PySpark is installed).

> **Healthcare Data Platform** (personal project, synthetic data) | Databricks, PySpark, Delta Lake, Unity Catalog, Auto Loader
> - Built a Bronze/Silver/Gold lakehouse for claims with Auto Loader ingestion, idempotent MERGE loads and a quarantine table that keeps every rejected row with its reasons; re-running a load changes nothing.
> - Reconciled [20,592] source rows to Silver and quarantine with zero unexplained difference, using a ground-truth manifest of injected defects, late arrivals and corrections.
> - Applied Unity Catalog column masks and a row filter to patient data, with default-deny role access; Gold tables carry no direct identifiers.
> - Surfaced [627] claims for services after coverage ended; wrote [64] automated tests, including a plain-Python cross-check of every Gold figure.

Leave out the RAG assistant until it exists. If you want to mention it, write "next: RAG assistant with evaluation"
under the project, never as a completed item. Your Ontario Health experience stays first on the resume.
Also add the repo link and pin the repo on GitHub, with the lineage and reconciliation screenshots in the README.

## Interview practice (90 seconds each, using this project)

- **How do you protect sensitive data in a lakehouse?** Minimize first (Gold has no identifiers), then column masks and a
  row filter on the table that does hold them, with default deny, so every read path is covered.
- **Mask versus view?** A table policy cannot be bypassed by reading the base table, and there is one definition to audit.
- **How do you stop a security policy from corrupting a pipeline?** Run the pipeline as a privileged identity and add a guard that
  fails loudly if it cannot see all the data. Mine compares visible patients with the unfiltered Bronze count.
- **How would this differ in production?** Account groups instead of a roles table, managed through your identity provider,
  tags and attribute-based policies instead of per-table setup, and audit logs reviewed on a schedule.
- **How does this relate to PHIPA-style requirements?** Data minimization, masking of identifiers, least-privilege access,
  and evidence of who can see what. Say what you built, and be clear it is a demonstration on synthetic data.

## Exercises

1. Add a mask for `postal_prefix` (a quasi-identifier) to `governance_sql.py`, with a test.
2. Add an `auditor` role that sees real values but no Gold, and extend the tests. Remember that a table can have only one row filter.
3. Write the one-minute explanation of why `silver_patients` is protected but `gold_claims_monthly` is not.

## Commit message

```
Add Unity Catalog column masks, row filter and role-based demo for patient data
```
