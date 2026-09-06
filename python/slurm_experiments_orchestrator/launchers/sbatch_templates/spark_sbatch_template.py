"""
Template for the Spark SLURM job script (standalone cluster: master + workers on
the allocation, then spark-submit in client mode).
"""

SBATCH_TEMPLATE = """#!/bin/bash -l
#SBATCH -J {name}-N{nodes}-{run_id}
#SBATCH -N {nodes}
#SBATCH --ntasks-per-node={executors_per_node}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem}
#SBATCH --exclusive
#SBATCH --time={walltime}
#SBATCH -A {account}
#SBATCH -p {partition}
#SBATCH --output={log_dir}/{run_id}_%j.out
#SBATCH --error={log_dir}/{run_id}_%j.err

set -euo pipefail

module purge && module load {spark_module}

CONFIG_PATH="{config_path}"
RUN_ID="{run_id}"
JOB_TAG="${{SLURM_JOB_ID}}_{run_id}"

# Pinned to align the daemon's actual footprint with the resolver's accounting.
export SPARK_DAEMON_MEMORY="{master_mem}g"

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

check_config() {{
    for file in "$CONFIG_PATH" "{jar_path}"; do
        if [ ! -f "$file" ]; then
            echo "ERROR: File not found - $file" >&2
            exit 1
        fi
    done
}}

setup_dirs() {{
    # SPARK_LOCAL_DIRS backs shuffle/cache. Must point to Lustre ($SCRATCH) 
    # to avoid exhausting compute nodes' minimal /tmp partitions.
    export SPARK_LOG_DIR="{output_dir}/spark_logs/$JOB_TAG"
    export SPARK_WORKER_DIR="/tmp/spark_work_$JOB_TAG"
    export SPARK_LOCAL_DIRS="{output_dir}/spark_local/$JOB_TAG"
    mkdir -p "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR" "$SPARK_LOCAL_DIRS"
}}

configure_spark() {{
    MASTER_HOST=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)

    # Hash-based deterministic ports to prevent clashes between co-scheduled jobs.
    # Modulo SLURM_JOB_ID caused collisions when jobs were spaced by exact multiples of 1000.
    MASTER_PORT=$(python3 -c "import hashlib, sys; print(49152 + int.from_bytes(hashlib.sha256(sys.argv[1].encode()).digest()[:2], 'big') % 16383)" "{run_id}")

    export SPARK_MASTER_HOST="$MASTER_HOST"
    export SPARK_MASTER_PORT="$MASTER_PORT"
    export SPARK_MASTER_WEBUI_PORT=$(( MASTER_PORT - 16384 ))
    export SPARK_WORKER_WEBUI_PORT=$(( MASTER_PORT - 32768 ))
    export SPARK_WORKER_MEMORY="{worker_mem}g"

    MASTER_URL="spark://$MASTER_HOST:$MASTER_PORT"
    MASTER_UI="http://$MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"
    
    EXECUTORS_PER_NODE={executors_per_node}
    TOTAL_EXECUTORS=$(( SLURM_NNODES * EXECUTORS_PER_NODE ))
    
    # 1 Worker DAEMON per node. Executors are spawned inside by the Master later.
    EXPECTED_WORKERS="$SLURM_NNODES"
    
    # Worker daemons MUST advertise the combined cores of all their packed executors.
    # Otherwise, portions of the SLURM node allocation remain unreachable to Spark.
    EXEC_CORES="${{SLURM_CPUS_PER_TASK:-{cpus_per_task}}}"
    WORKER_CORES=$(( EXEC_CORES * EXECUTORS_PER_NODE ))
    export SPARK_WORKER_CORES="$WORKER_CORES"
    TOTAL_CORES=$(( TOTAL_EXECUTORS * EXEC_CORES ))
}}

print_summary() {{
    echo "Config:          $CONFIG_PATH"
    echo "Master URL:      $MASTER_URL"
    echo "Master Web UI:   $MASTER_UI"
    echo "Nodes:           $SLURM_NNODES  (NODELIST=$SLURM_JOB_NODELIST)"
    echo "Executors:       $TOTAL_EXECUTORS  ($EXECUTORS_PER_NODE/node x $EXEC_CORES cores, $WORKER_CORES cores/Worker daemon)"
    echo "Executor memory: {executor_mem}g heap + {executor_overhead_mb}m overhead   driver: {driver_mem}g"
    echo "Daemon heap:     {master_mem}g each (Master + one Worker daemon per node)"
    echo "Shuffle width:   $TOTAL_CORES partitions (spark.sql.shuffle.partitions), AQE off"
}}

cleanup() {{
    EXIT_CODE=$?
    echo ">>> Trap EXIT (code=$EXIT_CODE) — cleaning up $RUN_ID"
    SPARK_IDENT_STRING="$JOB_TAG" \
        "$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true
    rm -rf "$SPARK_WORKER_DIR" "$SPARK_LOCAL_DIRS"
    exit $EXIT_CODE
}}

start_master() {{
    "$SPARK_HOME/sbin/start-master.sh"
}}

wait_for_master() {{
    echo -n "Waiting for Master to start"
    MAX_RETRIES=40
    SLEEP_SEC=2
    
    for _i in \$(seq 1 \$MAX_RETRIES); do
        LOG_FILE=\$(find "$SPARK_LOG_DIR" -name "*.out" 2>/dev/null | head -1)
        if [ -n "\$LOG_FILE" ] && grep -q "I have been elected leader" "\$LOG_FILE" 2>/dev/null; then
            echo " OK (\$((_i * SLEEP_SEC))s)"
            return 0
        fi
        echo -n "."
        sleep \$SLEEP_SEC
    done

    echo -e "\\nERROR: Master did not start in \$((MAX_RETRIES * SLEEP_SEC))s. Logs in $SPARK_LOG_DIR:"
    find "$SPARK_LOG_DIR" -name "*.out" 2>/dev/null | while read -r f; do
        echo "--- \$f ---"; tail -5 "\$f"
    done
    exit 1
}}

start_workers() {{
    srun --ntasks="$SLURM_NNODES" \
         --ntasks-per-node=1 \
         --cpus-per-task="$WORKER_CORES" \
         --export=ALL \
         --output="$SPARK_LOG_DIR/workers-%j-%t.out" \
         "$SPARK_HOME/bin/spark-class" \
         org.apache.spark.deploy.worker.Worker \
         --cores "$WORKER_CORES" \
         --memory "$SPARK_WORKER_MEMORY" \
         "$MASTER_URL" &
}}

wait_for_workers() {{
    echo -n "Waiting for $EXPECTED_WORKERS Worker daemons (REST $MASTER_UI/json/)"
    WORKERS_READY=0
    MAX_RETRIES=60
    SLEEP_SEC=2
    
    for _i in \$(seq 1 \$MAX_RETRIES); do
        WORKERS_READY=\$(curl -sf "$MASTER_UI/json/" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers', 0))" \
            2>/dev/null || echo 0)

        if [ "\$WORKERS_READY" -ge "$EXPECTED_WORKERS" ]; then
            echo " OK (\$WORKERS_READY/$EXPECTED_WORKERS in \$((_i * SLEEP_SEC))s)"
            return 0
        fi
        echo -n "."
        sleep \$SLEEP_SEC
    done

    echo -e "\\nERROR: Only \$WORKERS_READY/$EXPECTED_WORKERS Worker daemons ready. Stopping."
    echo "Cluster status (API):"
    curl -sf "$MASTER_UI/json/" 2>/dev/null \
        | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))" \
        2>/dev/null || echo "(API unavailable)"
    exit 1
}}

submit_job() {{
    # Gate on executors is handled internally by Spark via spark.scheduler.minRegisteredResourcesRatio
    spark-submit \
      --master "$MASTER_URL" \
      --deploy-mode client \
      --driver-memory "{driver_mem}g" \
      --executor-cores "$EXEC_CORES" \
      --executor-memory "{executor_mem}g" \
      --conf spark.executor.memoryOverhead={executor_overhead_mb}m \
      --conf spark.cores.max="$TOTAL_CORES" \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
      --conf spark.scheduler.maxRegisteredResourcesWaitingTime=120s \
      --conf spark.sql.shuffle.partitions="$TOTAL_CORES" \
      --conf spark.sql.adaptive.enabled=false \
      --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
      --conf spark.driver.extraJavaOptions=-XX:+UseG1GC \
      "{jar_path}" --config "$CONFIG_PATH"
}}

echo "=== RUN: $RUN_ID  (Job=$SLURM_JOB_ID) ==="
check_config
setup_dirs
configure_spark
trap cleanup EXIT INT TERM
print_summary
start_master
wait_for_master
start_workers
wait_for_workers
submit_job
"""
