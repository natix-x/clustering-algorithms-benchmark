# Benchmark contract

Single source of truth for the JSON exchanged between the **harness** (this repo)
and each **engine jar** (`spark-clustering-algorithms`, `flink-clustering-algorithms`).

```
experiment_configs/*.yaml ──(harness)──▶ <runId>.json  ──(engine jar: --config)──▶  <runId>.json (result)
                            run_config.schema.json                                  
```

The harness **writes** a per-run config that conforms to
[`run_config.schema.json`](run_config.schema.json). Each engine jar **reads** that
config, runs the job, and **writes** a result.

Only the **input** schema lives here: the harness produces `run_config`, so it owns and
validates `run_config.schema.json`. The **result** schema (`run_result.schema.json`) is
owned and validated in the engine repos — the harness never produces results, so it does
not ship that schema.

## Validating

Conformance is checked by **tests**, not at runtime, on the side that **produces** the
artifact: `tests/test_contract.py` validates every `build_run_config` output against
`run_config.schema.json`. The result is validated the same way in the engine repos.
