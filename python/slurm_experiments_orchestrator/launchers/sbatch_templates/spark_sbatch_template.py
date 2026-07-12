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

check_config() {{
    if [ ! -f "$CONFIG_PATH" ]; then
        echo "$CONFIG_PATH not found!"
        exit 1
    fi
}}

setup_dirs() {{
    # Isolated per-run log + worker scratch dirs.
    export SPARK_LOG_DIR="{output_dir}/spark_logs/$JOB_TAG"
    export SPARK_WORKER_DIR="/tmp/spark_work_$JOB_TAG"
    mkdir -p "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR"
}}

configure_spark() {{
    MASTER_HOST=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)

    # Deterministic per-run port in [49152, 65535] to avoid clashes between jobs.
    MASTER_PORT=$(python3 -c "
import hashlib, sys
seed = hashlib.sha256(sys.argv[1].encode()).digest()
print(49152 + int.from_bytes(seed[:2], 'big') % 16383)
" "{run_id}")

    export SPARK_MASTER_HOST="$MASTER_HOST"
    export SPARK_MASTER_PORT="$MASTER_PORT"
    export SPARK_MASTER_WEBUI_PORT=$(( 8080 + SLURM_JOB_ID % 1000 ))
    export SPARK_WORKER_WEBUI_PORT=$(( 8081 + SLURM_JOB_ID % 1000 ))
    export SPARK_WORKER_MEMORY="{worker_mem}g"
    export SPARK_WORKER_CORES="${{SLURM_CPUS_PER_TASK:-{cpus_per_task}}}"

    MASTER_URL="spark://$MASTER_HOST:$MASTER_PORT"
    MASTER_UI="http://$MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"
    CORES="${{SLURM_CPUS_PER_TASK:-{cpus_per_task}}}"
    EXECUTORS_PER_NODE={executors_per_node}
    TOTAL_EXECUTORS=$(( SLURM_NNODES * EXECUTORS_PER_NODE ))
    # Worker != Executor: we start exactly one long-lived Worker DAEMON per node (see
    # start_workers). Executors are JVM processes the Master spawns ON those Workers at
    # spark-submit time; their COUNT is driven by spark.cores.max / EXEC_CORES, not by the
    # Worker daemon count. So the readiness gate below waits for SLURM_NNODES daemons, not
    # TOTAL_EXECUTORS (aliveworkers can never exceed the number of Worker daemons started).
    EXPECTED_WORKERS="$SLURM_NNODES"
    # Cores PER EXECUTOR (granted explicitly in the matrix YAML). The worker advertises
    # all $CORES; standalone packs EXECUTORS_PER_NODE executors of EXEC_CORES each onto
    # it. The YAML author keeps cores and executor memory mutually consistent.
    EXEC_CORES={executor_cores}
    # spark.cores.max caps total cores Spark grabs cluster-wide; with EXEC_CORES per
    # executor this pins the executor COUNT (standalone ignores --num-executors).
    TOTAL_CORES=$(( TOTAL_EXECUTORS * EXEC_CORES ))
}}

print_summary() {{
    echo "Config:          $CONFIG_PATH"
    echo "Master URL:      $MASTER_URL"
    echo "Master Web UI:   $MASTER_UI"
    echo "Nodes:           $SLURM_NNODES  (NODELIST=$SLURM_JOB_NODELIST)"
    echo "Executors:       $TOTAL_EXECUTORS  ($EXECUTORS_PER_NODE/node x $EXEC_CORES cores)"
    echo "Executor memory: {executor_mem}g heap + {executor_overhead_mb}m overhead   driver: {driver_mem}g"
}}

cleanup() {{
    EXIT_CODE=$?
    echo ">>> Trap EXIT (kod=$EXIT_CODE) — cleaning up {run_id}"
    SPARK_IDENT_STRING="$JOB_TAG" \
        "$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || true
    rm -rf "$SPARK_WORKER_DIR"
    exit $EXIT_CODE
}}

start_master() {{
    "$SPARK_HOME/sbin/start-master.sh"
}}

wait_for_master() {{
    echo -n "Waiting for Master to start"
    MASTER_READY=""
    _i=0
    while [ "$_i" -lt 40 ]; do
        _i=$(( _i + 1 ))
        sleep 2
        LOG_FILE=$(find "$SPARK_LOG_DIR" -name "*.out" 2>/dev/null | head -1)
        if [ -n "$LOG_FILE" ] && grep -q "I have been elected leader" "$LOG_FILE" 2>/dev/null; then
            MASTER_READY="yes"
            echo " OK (${{_i}} s)"
            break
        fi
        echo -n "."
    done
    if [ -z "$MASTER_READY" ]; then
        echo ""
        echo "ERROR: Master did not start in 80s. Logs in  $SPARK_LOG_DIR:"
        find "$SPARK_LOG_DIR" -name "*.out" 2>/dev/null | while read -r f; do
            echo "--- $f ---"; tail -5 "$f"
        done
        exit 1
    fi
}}

start_workers() {{
    srun --ntasks="$SLURM_NNODES" \
         --ntasks-per-node=1 \
         --cpus-per-task="$CORES" \
         --export=ALL \
         --output="$SPARK_LOG_DIR/workers-%j-%t.out" \
         "$SPARK_HOME/bin/spark-class" \
         org.apache.spark.deploy.worker.Worker \
         --cores "$CORES" \
         --memory "$SPARK_WORKER_MEMORY" \
         "$MASTER_URL" &
}}

wait_for_workers() {{
    echo -n "Waiting for $EXPECTED_WORKERS Worker daemons (REST $MASTER_UI/json/)"
    WORKERS_READY=0
    for _i in $(seq 1 60); do
        WORKERS_READY=$(curl -sf "$MASTER_UI/json/" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('aliveworkers', 0))" \
        2>/dev/null || echo 0)

        if [ "$WORKERS_READY" -ge "$EXPECTED_WORKERS" ]; then
            echo " OK ($WORKERS_READY/$EXPECTED_WORKERS w ${{_i}} s)"
            break
        fi
        echo -n "."
        sleep 2
    done

    if [ "$WORKERS_READY" -lt "$EXPECTED_WORKERS" ]; then
        echo -e "\\nERROR: Only $WORKERS_READY/$EXPECTED_WORKERS Worker daemons ready. Stopping."
        echo "Cluster status (API):"
        curl -sf "$MASTER_UI/json/" 2>/dev/null \
        | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))" \
        2>/dev/null || echo "(API niedostepne)"
        exit 1
    fi
}}

submit_job() {{
    spark-submit \
      --master "$MASTER_URL" \
      --deploy-mode client \
      --driver-memory "{driver_mem}g" \
      --executor-cores "$EXEC_CORES" \
      --executor-memory "{executor_mem}g" \
      --conf spark.executor.memoryOverhead={executor_overhead_mb}m \
      --conf spark.cores.max="$TOTAL_CORES" \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      "{jar}" --config "$CONFIG_PATH"
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
