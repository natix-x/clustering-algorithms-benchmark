REQUIRED_KEYS = ("name", "configs_dir", "log_dir", "output_dir", "jar_path", "matrix")
REQUIRED_MATRIX_KEYS = ("nodes", "algorithm", "dataset")

#: Keys each matrix.dataset / matrix.algorithm entry must define (mirrors the run-config
#: contract: dataset {type, params}, algorithm {name, params}).
REQUIRED_DATASET_KEYS = ("type", "params")
REQUIRED_ALGORITHM_KEYS = ("name", "params")

#: Only `mem` is a SLURM size string (e.g. "48G"); every other resource key is a
#: plain positive integer (GB, MB, cores, or counts).
MEM_STRING_KEYS = ("mem",)

SLURM_RESOURCE_KEYS = ("cpus_per_task", "mem")

SPARK_RESOURCE_KEYS = SLURM_RESOURCE_KEYS + (
    "executors_per_node",
    "driver_mem_gb",
    "worker_mem_gb",
)

FLINK_RESOURCE_KEYS = SLURM_RESOURCE_KEYS + (
    "tm_per_node",
    "tm_mem_gb",
    "jm_mem_gb",
)
