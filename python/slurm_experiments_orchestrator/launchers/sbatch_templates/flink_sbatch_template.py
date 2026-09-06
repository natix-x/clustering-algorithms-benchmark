"""
Template for the Flink SLURM job script (standalone SESSION mode: a JobManager daemon on the
head node, a TaskManager per task, and a `flink run` client that runs main() in its own JVM).

Switched off Application Mode (05.09.2026): isolated `usrlib` pathing caused ClassNotFoundExceptions
on remote TMs. Session mode relies on BlobServer distribution, resolving this cleanly.
"""

FLINK_SBATCH_TEMPLATE = """#!/bin/bash -l
#SBATCH -J {name}-N{nodes}-{run_id}
#SBATCH -N {nodes}
#SBATCH --ntasks-per-node={tms_per_node}
#SBATCH --cpus-per-task={slots_per_tm}
#SBATCH --mem={mem}
#SBATCH --exclusive
#SBATCH --time={walltime}
#SBATCH -A {account}
#SBATCH -p {partition}
#SBATCH --output={log_dir}/{run_id}_%j.out
#SBATCH --error={log_dir}/{run_id}_%j.err

set -euo pipefail

module purge && module load {java_module}
export FLINK_HOME="{flink_home}"

CONFIG_PATH="{config_path}"
RUN_ID="{run_id}"
JAR_PATH="{jar_path}"

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

check_config() {{
    for file in "$CONFIG_PATH" "$JAR_PATH"; do
        if [ ! -f "$file" ]; then
            echo "ERROR: File not found - $file" >&2
            exit 1
        fi
    done
}}

setup_dirs() {{
    export CLUSTERING_METRICS_FILE="{output_dir}/metrics/$RUN_ID/flink-metrics.txt"
    export FLINK_CONF_DIR="{output_dir}/flink-conf/$RUN_ID"
    export FLINK_LOG_DIR="{output_dir}/flink_logs/$RUN_ID"

    mkdir -p "$(dirname "$CLUSTERING_METRICS_FILE")" "$FLINK_CONF_DIR" "$FLINK_LOG_DIR"
    cp -r "$FLINK_HOME"/conf/* "$FLINK_CONF_DIR"/ 2>/dev/null || true
}}

configure_cluster() {{
    JM_HOST=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
    TMS_PER_NODE={tms_per_node}
    SLOTS_PER_TM={slots_per_tm}
    TOTAL_TMS=$(( SLURM_NNODES * TMS_PER_NODE ))
    TOTAL_SLOTS=$(( TOTAL_TMS * SLOTS_PER_TM ))
    
    # Must sit on shared Lustre storage to ensure multi-node visibility and avoid
    # filling up minimal node-local /tmp partitions on compute nodes.
    FLINK_TMP_DIR="{output_dir}/flink_entrypoint/${{SLURM_JOB_ID}}_$RUN_ID"
    export FLINK_LOCAL_DIRS="{output_dir}/flink_local/${{SLURM_JOB_ID}}_$RUN_ID"
    mkdir -p "$FLINK_LOCAL_DIRS"
}}

setup_entrypoint_classpath() {{
    export FLINK_LIB_DIR="$FLINK_HOME/lib"
}}

write_flink_conf() {{
    # WARNINGS MAINTAINED FROM PREVIOUS RUNS:
    # 1. taskmanager.bind-host / host: Both required for cross-node TM reachability.
    # 2. default-source-parallelism: MUST equal $TOTAL_SLOTS. Otherwise, AdaptiveBatchScheduler 
    #    forces parallelism=1 on parquet sources, crippling load times (found 05.09.2026).
    # 3. task.off-heap.size: Set to 4GB. Sources (Parquet/Hadoop) allocate per-split direct 
    #    memory; without this, it consumes the 1GB network budget and OOMs.
    # 4. execution.attached: Required for detached EAGER client-side executions (count/collect).
    
    cat <<EOF >> "$FLINK_CONF_DIR/flink-conf.yaml"
jobmanager.rpc.address: $JM_HOST
jobmanager.rpc.port: 6123
jobmanager.bind-host: 0.0.0.0
taskmanager.bind-host: 0.0.0.0
rest.address: $JM_HOST
rest.bind-address: 0.0.0.0
rest.port: 8081
taskmanager.numberOfTaskSlots: $SLOTS_PER_TM
execution.batch.adaptive.auto-parallelism.default-source-parallelism: $TOTAL_SLOTS
taskmanager.memory.process.size: {tm_process_mb}m
taskmanager.memory.network.max: {tm_network_max_mb}m
taskmanager.memory.task.off-heap.size: 4096m
jobmanager.memory.process.size: {jm_process_mb}m
jobmanager.memory.heap.size: {jm_heap_mb}m
env.java.opts.all: -XX:+UseG1GC
parallelism.default: {parallelism}
io.tmp.dirs: $FLINK_LOCAL_DIRS
metrics.reporter.file.factory.class: clustering.metrics.FileMetricReporterFactory
metrics.reporter.file.path: $CLUSTERING_METRICS_FILE
metrics.reporter.file.interval: 200 MILLISECONDS
execution.attached: true
EOF
}}

print_summary() {{
    echo "JobManager/driver host: $JM_HOST   TaskManagers: $TOTAL_TMS   slots/TM: $SLOTS_PER_TM   total slots: $TOTAL_SLOTS   parallelism: {parallelism}"
    echo "TaskManager memory: {tm_process_mb}m process, split by Flink (network capped at {tm_network_max_mb}m)"
    echo "JobManager/driver:  {jm_process_mb}m process = {jm_heap_mb}m heap"
    echo "Spill dir (io.tmp.dirs): $FLINK_LOCAL_DIRS   logs: $FLINK_LOG_DIR"
}}

cleanup() {{
    EXIT_CODE=$?
    echo ">>> cleaning up $RUN_ID (exit=$EXIT_CODE)"
    if [ -n "${{JOB_PID:-}}" ]; then
        kill "$JOB_PID" 2>/dev/null || true
    fi
    "$FLINK_HOME/bin/taskmanager.sh" stop-all 2>/dev/null || true
    "$FLINK_HOME/bin/jobmanager.sh" stop 2>/dev/null || true
    rm -rf "$FLINK_TMP_DIR" "$FLINK_CONF_DIR" "${{FLINK_LOCAL_DIRS:-}}"
    exit $EXIT_CODE
}}

start_taskmanagers() {{
    srun --ntasks-per-node="$TMS_PER_NODE" \
         --cpus-per-task="$SLOTS_PER_TM" \
         --export=ALL \
         bash -c 'FLINK_LIB_DIR="$FLINK_HOME/lib" "$FLINK_HOME/bin/taskmanager.sh" start-foreground -D taskmanager.host=$(hostname)' &
}}

start_jobmanager() {{
    "$FLINK_HOME/bin/jobmanager.sh" start
}}

submit_job() {{
    "$FLINK_HOME/bin/flink" run \
      -m "$JM_HOST:8081" \
      -c clustering.benchmark.BenchmarkRunner \
      "$JAR_PATH" \
      "$CONFIG_PATH" &
    JOB_PID=$!
}}

wait_for_taskmanagers() {{
    echo -n "Waiting for $TOTAL_TMS TaskManagers (REST http://$JM_HOST:8081/overview)"
    REGISTERED=0
    SLEEP_SEC=2
    MAX_RETRIES=120

    for _i in \$(seq 1 \$MAX_RETRIES); do
        REGISTERED=\$(curl -sf "http://$JM_HOST:8081/overview" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" 2>/dev/null || echo 0)
        
        if [ "\$REGISTERED" -ge "$TOTAL_TMS" ]; then
            ELAPSED=\$((_i * SLEEP_SEC))
            echo " OK (\$REGISTERED/$TOTAL_TMS in \${{ELAPSED}}s)"
            break
        fi
        
        # Check if early job failure occurred
        if [ -n "${{JOB_PID:-}}" ] && ! kill -0 "$JOB_PID" 2>/dev/null; then
            echo -e "\nNOTE: Application process exited before all TaskManagers registered."
            return 0
        fi
        
        echo -n "."
        sleep \$SLEEP_SEC
    done

    if [ "\$REGISTERED" -lt "$TOTAL_TMS" ]; then
        echo -e "\nWARNING: Only \$REGISTERED/$TOTAL_TMS TaskManagers registered in \$((MAX_RETRIES * SLEEP_SEC))s."
        echo "Leaving the verdict to FlinkClusteringJob.waitForClusterReady. Diagnostics:"
        curl -sf "http://$JM_HOST:8081/overview" 2>/dev/null \
            | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))" \
            2>/dev/null || echo "(REST unavailable)"
            
        for _f in "$FLINK_LOG_DIR"/*standalonejob*.log; do
            [ -f "$_f" ] || continue
            echo "--- $_f ---"; tail -20 "$_f"
        done
    fi
}}

echo "=== RUN: $RUN_ID  (Job=$SLURM_JOB_ID) ==="
check_config
setup_dirs
configure_cluster
setup_entrypoint_classpath
write_flink_conf
trap cleanup EXIT INT TERM
print_summary
start_jobmanager
start_taskmanagers
wait_for_taskmanagers
submit_job
wait "$JOB_PID"
"""
