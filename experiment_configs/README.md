# Experiment YAML config files - format

Each file defines an experiment-matrix for benchmark sweeps. The harness expands the cartesian product of the following axes:
`nodes × resources × algorithms × datasets × repetitions`.

This generates one independent SLURM job per combination, each tagged with a unique runId.

## Top-level structure

| field | required | meaning                                                                                   |
|---|:---:|-------------------------------------------------------------------------------------------|
| `name` | ✔ | Prefix for generated runIds.                                                              |
| `configs_dir` | ✔ | Output directory for per-run `<runId>.json` files.                                          |
| `output_dir` | ✔ | Shared `$SCRATCH` path for results and engine logs (requires multi-node read/write access). |
| `log_dir` | ✔ | Path for SLURM stdout/stderr (`<runId>_%j.out / .err`).                                     |
| `jar_path` | ✔ | Path to the engine executable jar.                                                        |
| `experiment_matrix` | ✔ | Experiment configs.                                                                       |
| `repetitions` | ✔ | Positive integer; number of times to run each combination.                                |
| `sbatch_config` | – | SLURM/module defaults shared by every run                                                 |
| `evaluation_config` | – | Clustering-quality metric settings.                                                       |
| `spark_config` / `flink_config` | – | Additional engine properties (e.g., `spark.serializer: ...`).                                                                                          |

## Experiment configs (`experiment_matrix` & `evaluation_config`)

### `experiment_matrix`

- `nodes: [int]` The number of physical cluster nodes allocated per SLURM job (e.g., `[2, 4, 8]`)
- `resources: [dict]` Resource profiles (see [Resource management](#resource-management))
- `algorithms: [{name, params}]` (e.g., `kmeans` with params: `{k, maxIter}`).
- `datasets: [{type, params}]` (e.g., `synthetic` with `params: {numPoints}`, see [Datasets](#datasets)).

### `evaluation_config`
- `metrics` List of computed metrics (e.g., `silhouette`, `clusterSizes`, `noiseFraction`).
- `sampleSize` Sample size for computationally expensive metrics.
- `seed` RNG (random number generator) seed for reproducible sampling.

## Datasets

`params` are passed to the engine's DataSource unchanged. Env vars (`$SCRATCH`, ...) are expanded
at generation time, at any depth.

### `type: synthetic`

| param | required | meaning |
|---|:---:|---|
| `numPoints` | ✔ | Generated row count. |
| `seed` | – | RNG seed (default 42). |
| `numPartitions` | – | Generated partition count; injected from the resource profile if absent. |

### `type: parquet`

Reads the preprocessed datasets (see `datasets/README.md`). Each is one array column: `features`
for the tabular sets (Gaia, NYC), `emb` for the embedding ones (tech-news, monet, Cohere) — hence
`featureColumnName` is required.

| param | required | meaning |
|---|:---:|---|
| `path` | ✔ | Parquet file or directory. |
| `featureColumnName` | ✔ | Column holding the vector (`array<float>`, `array<double>` or an ML `Vector`). |
| `sampleFraction` | – | Fraction in (0, 1]; random subset |
| `seed` | – | Seed of `sampleFraction` (default 42). |
| `numPartitions` | – | Partition count after the read (coalesce down / shuffle up). Injected from the resource profile if absent; set it explicitly for partition-strategy sweeps. |
| `weightColumn` | – | Per-row weight (how many points the row stands for). Absent = 1.0 each. |

```yaml
datasets:
  - {type: parquet, params: {path: "/net/pr2/projects/plgrid/plggclustering25/gaia_data_preprocessed",
                             featureColumnName: features, sampleFraction: 0.01}}
  - {type: parquet, params: {path: "/net/pr2/projects/plgrid/plggclustering25/cohere_vectores_data_preprocessed",
                             featureColumnName: emb, sampleFraction: 0.05}}
```

## Environment (`sbatch_config`)

SLURM arguments and module setups applied to all generated jobs:
- Shared: `account`, `partition`, `walltime` (e.g., `"00:10:00"`).
- Spark: `spark_module` (e.g., `spark/3.3.2-hadoop-3.2-java-11`).
- Flink: `java_module` (e.g., `java/11.0.2` — Flink requires a manual installation on Ares) and `flink_home`.

## Resource management

| Parameter | Engine | Description |
|---|---|---|
| `cpus_per_task` | Both | Total cores allocated per SLURM task (Worker / TaskManager pool). |
| `mem` | Both | Total SLURM `--mem` per node (string, e.g., `"48G"`). |
| `executors_per_node` / `tm_per_node` | Spark / Flink | Instances of Executors / TaskManagers packed onto one node. |
| `worker_mem_gb` / `tm_mem_gb` | Spark / Flink | Total RAM pool granted to the Worker / TaskManager process. |
| `driver_mem_gb` / `jm_mem_gb` | Spark / Flink | Master process heap (Driver / JobManager). |

### Auto-Derived Values (Internal Logic)

**Spark**
- `executor_cores = cpus_per_task // executors_per_node`
- `executor_mem ≈ (worker_mem_gb / executors_per_node) / 1.10` (Reserves 10% for off-heap overhead)
- `spark.cores.max` = `nodes × executors_per_node × executor_cores`

**Flink**
- `total_slots = nodes × tm_per_node × cpus_per_task`
- `parallelism = total_slots`

**Resource Rules**
1. Keep `cpus_per_task strictly` divisible by `executors_per_node` (Spark) or `tm_per_node` (Flink). Fractions round down, leaving cores idle.
2. The combined `driver`/`master` memory and total `worker pool` memory must fit entirely within the SLURM `mem` limit to avoid node oversubscription and OOM kills.
