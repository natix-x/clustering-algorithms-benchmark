# clustering-algorithms-benchmark

Engine-agnostic **benchmark harness** for the MSc thesis comparing clustering
algorithms on Big Data frameworks. This repo owns experiment orchestration, the
exchange **contract**, and result analysis. The actual algorithm implementations
live in the per-engine repos:

- [`spark-clustering-algorithms`](https://github.com/natix-x/spark-clustering-algorithms) — Scala / Spark
- [`flink-clustering-algorithms`](https://github.com/natix-x/flink-clustering-algorithms) — Java / Flink

## How it fits together

One experiment-matrix YAML fans out into many independent per-run jobs. The harness
is engine-agnostic; `--framework` picks a **Launcher** (Strategy) that owns every
Spark/Flink-specific detail.

```mermaid
flowchart TB
    YAML["experiment_configs/&lt;name&gt;.yaml<br/>experiment_matrix: nodes × resources × algorithms × datasets (+ repetitions)"]

    subgraph HARNESS["HARNESS (Python) — run_experiments.py --framework {spark|flink} [--submit]"]
        direction TB
        S1["1. launchers.get_launcher(fw)<br/>dict factory → SparkLauncher / FlinkLauncher (Strategy)"]
        S2["2. validate_yaml_config_file(...)<br/>yaml_validator: INPUT validation<br/>resource_keys come from chosen launcher (SPARK_/FLINK_*)"]
        S3["3. generate_experiments(doc)<br/>cartesian product → list[Experiment]<br/>nodes×resources×algorithms×datasets×reps, each a unique runId"]
        S4["4. JobWriter(launcher)<br/>Strategy Context, delegates to launcher"]
        S4a["write_configs → launcher.build_run_config() per run<br/>derives parallelism (= total slots/cores), injects numPartitions<br/>writes &lt;configs_dir&gt;/&lt;runId&gt;.json (the CONTRACT shape)"]
        S4b["write_sbatch_files → launcher.render_sbatch() per run<br/>fills sbatch_templates/{spark,flink}_sbatch_template.py<br/>writes &lt;out&gt;/sbatch/&lt;runId&gt;.sbatch + submit_all.sh"]
        S5["5. --submit → bash submit_all.sh → sbatch &lt;runId&gt;.sbatch (per run)"]

        S1 --> S2 --> S3 --> S4
        S4 --> S4a
        S4 --> S4b
        S4a --> S5
        S4b --> S5
    end

    YAML --> HARNESS

    subgraph SLURM["each &lt;runId&gt;.sbatch (SLURM job)"]
        direction TB
        SB1["bootstraps a throwaway standalone cluster ON the allocation"]
        SB2["Spark: start Master + srun Workers → spark-submit --config &lt;runId&gt;.json"]
        SB3["Flink: start JobManager + srun TaskManagers → flink run --config ..."]
        SB4["trap cleanup on EXIT/INT/TERM → tear down cluster, rm scratch"]
        SB1 --> SB2
        SB1 --> SB3
        SB2 --> SB4
        SB3 --> SB4
    end

    HARNESS -- "sbatch (SLURM)" --> SLURM

    JAR["engine fat jar<br/>(lives in the engine repo)"]
    SLURM --> JAR

    RESULT["&lt;output_dir&gt;/&lt;runId&gt;.json (result)<br/>validates against contract/run_result.schema.json"]
    JAR --> RESULT

    ANALYSIS["analysis/ (pandas, plots)"]
    RESULT --> ANALYSIS

```

### The contract (why this stays decoupled)

`contract/*.schema.json` is the single, language-neutral spec exchanged between the
three repos. The harness **produces** `run_config` JSON; each engine jar **reads** it
and **produces** `run_result` JSON:

```
                       contract/run_config.schema.json  ◄─ produced by harness
                                    ▲                       (checked in tests/)
   harness (Python) ───────────────┤
                                    ▼
   spark-clustering-algorithms (Scala)  ──►  contract/run_result.schema.json
   flink-clustering-algorithms (Java)   ──►         ▲ produced by each engine jar
                                                    └─ mirrored by hand in each repo
```

No shared JVM library: each engine keeps its own small parser and conforms to the
schema. Adding an engine = one `Launcher` subclass + its resource-key tuple + one
`_LAUNCHERS` entry — nothing in the harness core changes.

## Layout

```
contract/                         # JSON Schemas: the source of truth (see contract/README.md)
experiment_configs/               # YAML experiment matrices (engine-neutral)
analysis/                         # result analysis + plots
local_testing/experiment_configs/ # example per-run configs (contract fixtures)
python/slurm_experiments_orchestrator/
  run_experiments.py              # CLI entry: --framework {spark,flink} matrix.yaml [--submit]
  common/                         # engine-agnostic: matrix expansion, YAML validation, config keys
  slurm/jobs_writer.py            # JobWriter (Strategy Context): delegates engine specifics
  launchers/
    base_launcher.py              # Launcher ABC (Strategy)
    __init__.py                   # get_launcher/available dict factory
    spark_launcher.py + spark_resources_resolver.py   # Spark sizing + config/sbatch
    flink_launcher.py             # Flink launcher (sbatch is a Faza 3 TODO)
    sbatch_templates/             # spark_sbatch_template.py, flink_sbatch_template.py
tests/                            # pytest suite (validator, generator, launchers, contract)
```

## Usage

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync                 # create .venv with runtime + dev deps
uv run pytest           # run the test suite
```

On the cluster, use the thin wrapper (`slurm_run.sh` just sets PYTHONPATH and forwards
args to the Python entrypoint):

```bash
# generate configs + sbatch, then submit all jobs:
./slurm_run.sh -f flink experiment_configs/flink_kmeans_example.yaml --submit

# or just generate (prints the submit_all.sh path to run later):
./slurm_run.sh -f spark experiment_configs/spark_kmeans_example.yaml
```

Input YAML is validated up front (`yaml_validator`). Generated per-run configs are
checked against the contract in the test suite (`tests/test_contract.py`), not at
runtime.

## Contract

See [`contract/README.md`](contract/README.md). In short: each engine jar reads the
same `RunConfig` JSON and writes the same `RunResult` JSON, so analysis is uniform
across engines. Fields are tagged CORE (engine-neutral, mandatory) vs ENGINE
(Spark-flavoured counters; Flink emits the analogue or 0).