# clustering-algorithms-benchmark

Part of the Master thesis 'Performance and efficiency issues of the use of Big Data frameworks for implementation of clustering algorithms'. 

## Table of contents
* [General info](#general-info)
* [Architecture](#architecture)
* [Project structure](#project-structure)
* [Requirements](#requirements)
* [Usage](#usage)
* [Contract](#contract)


### General info
Engine-agnostic **benchmark harness** for the MSc thesis comparing clustering
algorithms on Big Data frameworks. This repo owns experiment orchestration, the
exchange **contract**, and result analysis. The actual algorithm implementations
live in the per-engine repos:

- [`spark-clustering-algorithms`](https://github.com/natix-x/spark-clustering-algorithms) — Scala / Spark
- [`flink-clustering-algorithms`](https://github.com/natix-x/flink-clustering-algorithms) — Java / Flink

## Architecture

One experiment-matrix YAML fans out into many independent per-run jobs. The harness
is engine-agnostic; `--framework` picks a **Launcher** (Strategy) that owns every
Spark/Flink-specific detail.

### Flowchart:

```mermaid
---
config:
  layout: dagre
---
flowchart TB
    START_DOT((( ))) --> YAML
    
    YAML["experiment_configs/&lt;name&gt;.yaml<br>nodes × resources × algorithms × datasets × repetitions"]
    START["START: slurm_experiments_orchestrator.run_experiments.py --framework {spark|flink} [--submit]"]
    
    InitSpark["inicjalizacja SparkLauncher"]
    InitFlink["inicjalizacja FlinkLauncher"]
    
    V["validate_yaml_config_file(parsed_yaml_config, launcher)<br>walidacja pliku konfiguracyjnego YAML dla konkretnego frameworka"]
    G["generate_experiments(parsed_yaml_config)<br>iloczyn kartezjański nodes × resources × algorithms × datasets × repetitions → list[Experiment], każdy z unikalnym runId"]
    JW["inicjalizacja JobWriter(launcher)<br>Deleguje pracę do launchera"]
    
    WC["① write_configs(...) → launcher.build_run_config()<br>przygotowuje metadane eksperymentu (zasoby, ścieżki, konfiguracje itp.), Job Writer zapisuje je jako &lt;runId&gt;.json"]
    WS["② write_sbatch_files(...) → launcher.render_sbatch()<br>wypełnia szablon sbatch metadanymi eksperymentu, zapisuje &lt;runId&gt;.sbatch + submit_all.sh, który pozwoli później uruchomić wszystkie joby na raz"]
    
    RUN["submit_all.sh → uruchamia sbatch &lt;runId&gt;.sbatch dla każdego eksperymentu"]
    END_NODE["zwraca gotowe pliki"]

    YAML --> START
    
    q1_choice{"--framework = ?"}
    START --> q1_choice
    q1_choice -- spark --> InitSpark
    q1_choice -- flink --> InitFlink
    
    join_launchers(("launcher"))
    InitSpark --> join_launchers
    InitFlink --> join_launchers
    
    join_launchers --> V
    
    V --> G
    G --> JW
    JW --> WC
    WC --> WS
    
    sub_choice{"--submit?"}
    WS --> sub_choice
    sub_choice -- tak --> RUN
    sub_choice -- nie --> END_NODE
    
    END_DOT((( )))
    RUN --> END_DOT
    END_NODE --> END_DOT
```

### UML Class Diagram - Strategy Pattern
```mermaid
---
config:
  layout: dagre
---
classDiagram
    direction TB
    
    class JobWriter {
        -_launcher : Launcher
        +write_configs() void
        +write_sbatch_files() void
    }
    note for JobWriter "Kontekst wzorca realizujący delegację zadań"

    class SparkLauncher {
        +name : str = "spark"
        +resource_keys : tuple = SPARK_RESOURCE_KEYS
        +build_run_config() dict
        +render_sbatch() str
    }
    
    class FlinkLauncher {
        +name : str = "flink"
        +resource_keys : tuple = FLINK_RESOURCE_KEYS
        +build_run_config() dict
        +render_sbatch() str
    }
    
    class Launcher {
        <<abstract>>
        +name : str
        +resource_keys : "tuple[str, ...]"
        +build_run_config()* dict
        +render_sbatch()* str
    }
    note for Launcher "Abstrakcyjna klasa bazowa (Strategia)"

    %% JobWriter korzysta z interfejsu wystawionego przez klasę bazową
    JobWriter --> Launcher : Korzysta ze strategii
    
    %% Klasy konkretne rozszerzają abstrakcyjną klasę bazową
    SparkLauncher --|> Launcher : rozszerza
    FlinkLauncher --|> Launcher : rozszerza
```

### Cluster topology on SLURM

Every generated `<runId>.sbatch` bootstraps a **throwaway standalone cluster on the
allocated nodes**, runs one job, then tears it down (`trap cleanup` on exit). There is
no external cluster manager — each SLURM job owns its cluster for its lifetime, which
keeps runs isolated and reproducible.

- **Spark** — a Master on the head node + one Worker per node (started via `srun`). The
  driver runs in client mode on the head node; executors are packed onto the Workers.
- **Flink** — a JobManager on the head node + one TaskManager per task (via `srun`).
  Job parallelism = total slots = `nodes × tm_per_node × cpus_per_task`.

<p align="center">
  <img src="media/spark_standalone_ares_cluster.png" alt="Spark standalone cluster on Ares" width="80%"><br>
  <em>Spark standalone cluster on a SLURM allocation (Ares), shown from the
  <strong>master–slave role</strong> perspective.</em>
</p>

<p align="center">
  <img src="media/flink_standalone_ares_cluster.png" alt="Flink standalone session cluster on Ares" width="80%"><br>
  <em>Flink standalone session cluster on a SLURM allocation (Ares), shown from the
  <strong>physical-node</strong> perspective.</em>
</p>

### The contract

`contract/*.schema.json` is the **single, language-neutral source of truth** for the
JSON exchanged between the three repos. Two schemas, one per direction:

- `run_config.schema.json` — the per-run input the harness **produces** and each engine
  jar **consumes** (via `--config <runId>.json`).
- `run_result.schema.json` — the result each engine jar **produces**, read uniformly by
  the analysis code.

```
                   run_config.schema.json                 run_result.schema.json
                          (input)                                (output)
   harness (Python)  ───writes──►  <runId>.json  ──►  engine jar  ───writes──►  <runId>.json (result)
                                                     (Spark / Flink)                    │
                                                                                        ▼
                                                                                 analysis (pandas)
```

The schema is **not a shared JVM library**: each engine keeps its own small
parser/serializer (Scala `case class`, Java POJO) that mirrors the schema by hand — no
cross-language build. Conformance is enforced by tests, not at runtime:

- harness side: `tests/test_contract.py` checks that every `build_run_config` output
  validates against `run_config.schema.json`;
- engine side: each engine repo validates a sample result against `run_result.schema.json`.

Adding an engine = one `Launcher` subclass + its resource-key tuple + one `_LAUNCHERS`
entry — nothing in the harness core changes.

## Project structure

```
contract/                         # JSON Schemas: the source of truth (see contract/README.md)
experiment_configs/               # YAML experiment matrices (engine-neutral)
local_testing/experiment_configs/ # example per-run configs (contract fixtures)
python/
  slurm_experiments_orchestrator/ # job generation + submission (the harness)
    run_experiments.py            # CLI entry: --framework {spark,flink} <matrix>.yaml [--submit]
    common/                       # engine-agnostic: matrix expansion, YAML validation, config keys
    slurm/jobs_writer.py          # JobWriter (Strategy Context): delegates engine specifics
    launchers/
      base_launcher.py            # Launcher ABC (Strategy)
      __init__.py                 # get_launcher/available dict factory
      spark_launcher.py + spark_resources_resolver.py   # Spark sizing + config/sbatch
      flink_launcher.py           # Flink launcher
      sbatch_templates/           # spark_sbatch_template.py, flink_sbatch_template.py
  data_preprocessing/             # dataset preparation
  data_analysis/                  # result loading, metrics comparison, plots
tests/                            # pytest suite (validator, generator, launchers, contract)
media/                            # images, tables, etc. used accross repository
```

## Requirements

- Python 3.9+ 
- pyyaml
- jsonschema
- pandas
- numpy
- matplotlib
- pytest (for testing)

## Usage

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (fast Python package
manager).

**1. Install uv** (once):

```bash
UV_VERSION="0.9.28" && curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | sh
```

See the [uv documentation](https://docs.astral.sh/uv/) if you hit any issues.

**2. Set up the environment:**

```bash
uv sync           
uv run pytest   
```

`uv sync` installs the runtime deps (`pyyaml`, `jsonschema`) plus the `dev` group
(`pytest`). Add `--extra analysis` for the analysis extras (`pandas`, `numpy`,
`matplotlib`).

**3. Generate / submit jobs.** On the cluster use the thin wrapper (`slurm_run.sh` just
sets `PYTHONPATH` and forwards args to the Python entrypoint):

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