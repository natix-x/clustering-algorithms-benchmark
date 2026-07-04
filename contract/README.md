# Benchmark contract

Single source of truth for the JSON exchanged between the **harness** (this repo)
and each **engine jar** (`spark-clustering-algorithms`, `flink-clustering-algorithms`).

```
matrix.yaml ──(harness)──▶ <runId>.json  ──(engine jar: --config)──▶  <runId>.json (result)
              run_config.schema.json                                   run_result.schema.json
```

The harness **writes** a per-run config that validates against
[`run_config.schema.json`](run_config.schema.json). Each engine jar **reads** that
config, runs the job, and **writes** a result that validates against
[`run_result.schema.json`](run_result.schema.json). Analysis then reads results
uniformly across engines.

## Why a spec (Option A) instead of a shared library

Spark is Scala, Flink is Java — a shared JVM contract lib would force a
cross-language build + submodule wiring. Instead, **each engine keeps its own small
parser/serializer** and conforms to these schemas. Drift is caught by a validation
test in each engine repo (output must validate against `run_result.schema.json`).
If the schema starts churning, we can promote this to a plain-Java contract jar later.

## CORE vs ENGINE fields

`run_result.schema.json` tags every field:

- **CORE** — engine-neutral. Every engine MUST emit these with the same meaning
  (identity, timings, workload shape, clustering quality). These are what the thesis
  comparison primarily relies on.
- **ENGINE** — Spark-flavoured counters (shuffle/spill/stage/executor metrics).
  Flink emits the closest analogue where one exists, otherwise `0`. The schema keeps
  the same column set across engines so pandas sees stable columns; see each field's
  `description` for the Spark→Flink mapping.

### Engine config key

`sparkConf` is the historical key (still emitted by the Spark jar). `engineConf` is
the neutral alias. Analysis accepts either; new Flink output should prefer
`engineConf`. Both are declared in the schemas.

## Validating locally

```bash
# any result file
python -m jsonschema -i <result>.json contract/run_result.schema.json   # pip install jsonschema
```

The harness validates generated configs against `run_config.schema.json` before
writing them; each engine repo has a test validating a sample result against
`run_result.schema.json`.