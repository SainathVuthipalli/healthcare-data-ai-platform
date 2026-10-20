# Day 1: seeded data with known defects

## Learn (30 minutes)

Read about **idempotency** and **ground truth** for pipelines, then answer in your own words:

1. If the same file is loaded twice, why should the target table not change?
2. Why is a manifest of injected defects more useful than "it looked fine"?
3. What is the difference between a late-arriving record and a correction?

## Build (done)

- `src/generate_data.py`: seeded generator for patients, providers, eligibility and two claim batches.
- `tests/test_generator.py`: 39 test cases (12 tests, four seeds for the main ones).

## Exercises (do these yourself)

1. Run `python src/generate_data.py --seed 7 --out data/seed7` and confirm the files differ from seed 42.
2. Add a new defect, `future_service_date` (a service date after the end of period 2), to
   `DEFECT_ACTIONS`, `_apply_defect` and the tests. Watch the "distinct rows" test protect you.
3. In a notebook or plain Python, load `claims_batch_1.csv` and count each defect yourself.
   Compare with `manifest.json`.

## Two bugs the tests caught (interview material)

1. **Corrections older than the original.** Corrections used a fixed timestamp, but many
   original claims had later `updated_at` values. A correction that is older than its
   original would be ignored by a latest-wins MERGE. Fix: derive each correction's timestamp
   from its original.
2. **A defect that silently did nothing.** The `invalid_dx_format` injection removed the dot
   from a code, but one code (`I10`) has no dot, so the "bad" row stayed valid and the
   manifest overstated the defects. It only showed once the random sequence changed. Fix: make
   the mutation invalid for every code, then run the tests across several seeds.

The lesson for both: ground truth you generate must itself be tested.

## Interview practice (answer out loud, 90 seconds each)

- How do you make a pipeline idempotent?
- How do you handle late-arriving data in an incremental load?
- How would you prove that no rows were lost between source and target?

## Commit message

```
Add seeded synthetic claims generator with injected defects and tests
```
