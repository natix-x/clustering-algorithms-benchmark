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
| `max_concurrent_runs` | – | Caps how many runs of this matrix execute at once. Unset = SLURM schedules everything at once. |
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
- `metrics` List of computed metrics: `silhouette`, `nClusters`, `clusterSizes`, `noiseFraction`.
  Omitting the key (or the whole `evaluation_config`) means all four. To run fit-only:
  ```yaml
  evaluation_config:
    metrics: []
  ```
- `sampleSize` Cap for the O(n²) silhouette. Default 10 000 on both engines.
- `seed` Base RNG seed for the evaluation draw (default 42). The launcher writes `seed + rep` into
  each generated run config, so every cell of one repetition shares a seed and repetitions differ.

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
| `numPartitions` | – | Partition count after the read (coalesce down / shuffle up). Injected from the resource profile if absent. |
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
| `cpus_per_task` | Both | Cores per SLURM task — cores per **executor** (Spark) / slots per **TaskManager** (Flink). Not split across instances. |
| `mem` | Both | Total SLURM `--mem` per node (string, e.g., `"48G"`). |
| `executors_per_node` / `tm_per_node` | Spark / Flink | Executors / TaskManagers packed onto one node. |
| `worker_mem_gb` / `tm_mem_gb` | Spark / Flink | RAM pool per node for the Worker / TaskManager processes. |
| `driver_mem_gb` / `jm_mem_gb` | Spark / Flink | Driver budget: a **heap** for Spark, a **process total** for Flink. Set `jm_mem_gb ≈ driver_mem_gb + 1`. |