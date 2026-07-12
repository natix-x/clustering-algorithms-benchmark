# Benchmark contract

Single source of truth for the JSON exchanged between the **harness** (this repo)
and each **engine jar** (`spark-clustering-algorithms`, `flink-clustering-algorithms`).

```
experiment_configs/*.yaml ──(harness)──▶ <runId>.json  ──(engine jar: --config)──▶  <runId>.json (result)
                            run_config.schema.json                                  
```

The harness **writes** a per-run config that conforms to
[`run_config.schema.json`](run_config.schema.json). Each engine jar **reads** that
config, runs the job, and **writes** a result that conforms to

## Why a spec (Option A) instead of a shared library

Spark is Scala, Flink is Java — a shared JVM contract lib would force a
cross-language build + submodule wiring. Instead, **each repo keeps its own small
parser/serializer** and conforms to these schemas:

- the harness (Python) builds the config as plain dicts;
- the Spark jar mirrors it with a Scala `case class`;
- the Flink jar mirrors it with a Java POJO.

Drift is caught by tests (see *Validating* below), not by a shared type. If the schema
starts churning, we can promote it to a plain-Java contract jar later.

## CORE vs ENGINE fields

`run_result.schema.json` tags every field:

- **CORE** — engine-neutral. Every engine MUST emit these with the same meaning
  (identity, timings, workload shape, clustering quality). These are what the thesis
  comparison primarily relies on.
- **ENGINE** — Spark-flavoured counters (shuffle/spill/stage/executor metrics).
  Flink emits the closest analogue where one exists, otherwise `0`. The schema keeps
  the same column set across engines so pandas sees stable columns; see each field's
  `description` for the Spark→Flink mapping.

### Engine config keys

Engine-specific tuning lives under **per-engine keys** in `run_config`: the Spark jar
reads `spark_config`, the Flink jar reads `flink_config` (each a `{string: string}`
map, both optional). A run only carries the key for its own engine.

## Validating

Conformance is checked by **tests**, at the side that **produces** each artifact:

- **`run_config`** (produced by the harness): `tests/test_contract.py` validates every
  `build_run_config` output against `run_config.schema.json`.
- **`run_result`** (produced by each engine jar): validated in the engine repos, against
  `run_result.schema.json` — the harness does not produce results, so it does not check
  them.
