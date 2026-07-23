# clustering-algorithms-benchmark

Part of the Master thesis 'Performance and efficiency issues of the use of Big Data frameworks for implementation of clustering algorithms'. 

## Table of contents
* [General info](#general-info)
* [Architecture](#architecture)
* [Datasets](#datasets)
* [Project structure](#project-structure)
* [Requirements](#requirements)
* [Usage](#usage)


### General info
Engine-agnostic **benchmark harness** for the MSc thesis comparing clustering
algorithms on Big Data frameworks. This repo owns experiment orchestration, the
exchange **contract**, and result analysis. The actual algorithm implementations
live in the per-engine repos:

- [`spark-clustering-algorithms`](https://github.com/natix-x/spark-clustering-algorithms) — Scala / Spark
- [`flink-clustering-algorithms`](https://github.com/natix-x/flink-clustering-algorithms) — Java / Flink

A language-neutral JSON contract couples the harness with each engine jar: the harness
produces `run_config`, each engine consumes it and produces a result. The format, the
CORE vs ENGINE fields, and how conformance is validated are documented in
[`contract/README.md`](contract/README.md).

> **Note:** Some documentation and diagrams are in Polish, as the master thesis they accompany is written in Polish.

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

#### Spark bootstrap (inside each `<runId>.sbatch`)

`spark-submit` runs in **client mode** on the head node; the trap tears the cluster
down on any exit.

```mermaid
sequenceDiagram
  autonumber
  actor SLURM as SLURM

  box Węzeł Główny 
    participant Script as Skrypt sbatch
    participant Master as Spark Master
    participant App as Spark Driver
  end

  box Węzły Obliczeniowe 
    participant Workers as Spark Workers
    participant Executors as Spark Executors
  end

  SLURM->>Script: Przydział zasobów i inicjalizacja skryptu
  activate Script
  Note over Script,Executors: 1. Inicjalizacja środowiska
  Script->>Script: Konfiguracja środowiska (host i porty Mastera, pula CPU/RAM, katalogi robocze, trap EXIT)

  Note over Script,Executors: 2. Uruchomienie klastra
  Script-)Master: Inicjalizacja Spark Mastera (start-master.sh)
  activate Master
  loop Oczekiwanie na gotowość Mastera
    Script->>Script: Sprawdzanie logów 
  end
  Script-)Workers: Równoległe uruchomienie Workerów na węzłach
  activate Workers
  Workers-)Master: Rejestracja workerów w klastrze (deklaracja puli CPU i RAM)

  Note over Script,Executors: 3. Weryfikacja gotowości
  loop Weryfikacja stanu klastra
    Script->>Master: Odpytanie REST API o status infrastruktury
    Master-->>Script: Potwierdzenie dostępności N węzłów roboczych
  end

  Note over Script,Executors: 4. Wysłanie zadania i obliczenia
  Script->>App: Delegacja zadania obliczeniowego (spark-submit w trybie client)
  activate App
  App-)Master: Rejestracja kontekstu (SparkContext) i żądanie alokacji zasobów
  Master-)Workers: Zlecenie uruchomienia executorów na Workerach
  Workers-)Executors: Uruchomienie procesów JVM (Spark Executors)
  activate Executors
  Executors-->>App: Zwrócenie rezultatów i statusu wykonania
  deactivate Executors
  App-->>Script: Zakończenie pracy
  deactivate App

  Note over Script,Executors: 5. Sprzątanie (trap EXIT)
  Script-)Master: Zakończenie procesu Mastera (stop-master.sh)
  deactivate Master
  Script-)Workers: Usunięcie tymczasowych obszarów roboczych
  deactivate Workers
  Script-->>SLURM: Zakończenie joba i zwolnienie przydzielonych węzłów
  deactivate Script
```

#### Flink bootstrap (inside each `<runId>.sbatch`)
TODO: przyjrzyj jeszcze raz ten diagram - czy ma sens krok z ususwaniem TaskManagerów osobno ??? 

Standalone **session** cluster: `flink run` submits to the JobManager REST endpoint;
the trap stops all daemons on exit.

```mermaid
sequenceDiagram
  autonumber
  actor SLURM as SLURM

  box Węzeł Główny
    participant Skrypt as Skrypt sbatch
    participant JM as JobManager
    participant Client as Flink Client
  end

  box Węzły Obliczeniowe
    participant TM as TaskManagers
  end

  participant Storage as Współdzielony dysk

  SLURM->>Skrypt: Przydział zasobów i inicjalizacja skryptu
  activate Skrypt
  Note over Skrypt,Storage: 1. Inicjalizacja środowiska
  Skrypt-)Storage: Wygenerowanie flink-conf.yaml (pamięć, porty, reporter metryk)

  Note over Skrypt,TM: 2. Uruchomienie klastra
  Skrypt-)JM: Uruchomienie procesu JobManagera
  activate JM
  Storage-->>JM: Załadowanie konfiguracji (otwarcie portów RPC/REST)
  Skrypt-)TM: Równoległy start N TaskManagerów
  activate TM
  Storage-->>TM: Załadowanie spójnej konfiguracji
  TM-)JM: Rejestracja dostępnych Task Slots

  Note over Skrypt,TM: 3. Weryfikacja gotowości
  loop Weryfikacja stanu klastra
    Skrypt->>JM: Odpytanie REST API (GET /overview) o stan zasobów
    JM-->>Skrypt: Potwierdzenie dostępności oczekiwanej liczby TaskManagerów
  end

  Note over Skrypt,TM: 4. Wysłanie zadania i obliczenia
  Skrypt->>Client: Zlecenie uruchomienia zadania (flink run)
  activate Client
  Client-)JM: Wysłanie aplikacji (plik JAR) oraz JobGraph
  JM-)TM: Dystrybucja podzadań (Tasks) do zarejestrowanych slotów
  TM-)Storage: Asynchroniczny zapis logów i metryk klastrowania
  Client-->>Skrypt: Potwierdzenie zakończenia
  deactivate Client

  Note over Skrypt,TM: 5. Sprzątanie (trap EXIT)
  Skrypt-)TM: Zatrzymanie TaskManagerów
  deactivate TM
  Skrypt-)JM: Zatrzymanie JobManagera
  deactivate JM
  Skrypt-)Storage: Usunięcie tymczasowych plików konfiguracyjnych klastra
  Skrypt-->>SLURM: Zakończenie joba i zwolnienie przydzielonych węzłów
  deactivate Skrypt
```

#### Flink one-time setup on Ares

Unlike Spark, Flink is **not** available as an Ares module,
and Flink loads metric reporters from its **own** classpath (`$FLINK_HOME/lib`) rather than
from the job jar. So a Flink installation and the reporter jar must be prepared **once per
install**, before the first run. The YAML `flink_home` points at this install
(e.g. `$SCRATCH/flink-1.17.1`).

**1. Install Flink on shared scratch.** Use 1.17.1 (matches the jar's `flink.version`) on
Java 11 — the common runtime with Spark 3.3, for a consistent comparison:

```bash
cd "$SCRATCH"
wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
tar xzf flink-1.17.1-bin-scala_2.12.tgz        # -> $SCRATCH/flink-1.17.1 (= flink_home)
```

**2. Build and install the metric-reporter jar.** Built from the
`flink-clustering-algorithms` repo; it is slim (depends only on `flink-metrics-core`,
already shipped with Flink) and must live on the cluster classpath, not in the job jar:

```bash
cd /path/to/flink-clustering-algorithms/flink
mvn -o package
cd target/classes
jar cf ../flink-clustering-metrics-reporter.jar \
  clustering/metrics \
  META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory
cp ../flink-clustering-metrics-reporter.jar "$SCRATCH/flink-1.17.1/lib/"
```

## Datasets

The benchmark runs on five public datasets. Their descriptions, sizes,
licenses, and preprocessing are documented in [`datasets/README.md`](datasets/README.md);
the preprocessing jobs live in `python/data_preprocessing/` with SLURM scripts in
`sbatch_scripts/`.

## Project structure

```
contract/                         # run_config.schema.json: the input contract (see contract/README.md)
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

The experiment YAML format is documented in [`experiment_configs/README.md`](experiment_configs/README.md).
<br/>
You can also check out the real example config files that are provided in `experiment_configs/` directory.
