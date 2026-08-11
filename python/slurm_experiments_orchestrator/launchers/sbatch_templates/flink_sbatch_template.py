"""
Template for the Flink SLURM job script (standalone session cluster: one
JobManager on the head node + a TaskManager per task, then `flink run`).

The bash body is split into single-purpose functions (check_config, setup_dirs,
configure_cluster, write_flink_conf, start_jobmanager, start_taskmanagers,
wait_for_taskmanagers, submit_job, cleanup) with a short `main` flow at the bottom.
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
# No Flink module on Ares -> use a self-installed Flink (FLINK_HOME) + a Java 11 module
# (Java 11 = common runtime with Spark 3.3 for a consistent comparison).
module purge && module load {java_module}
export FLINK_HOME="{flink_home}"

CONFIG_PATH="{config_path}"
RUN_ID="{run_id}"

# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #

check_config() {{
    if [ ! -f "$CONFIG_PATH" ]; then
        echo "$CONFIG_PATH not found!"
        exit 1
    fi
}}

setup_dirs() {{
    # SHARED metrics file (Lustre $SCRATCH via output_dir): every TaskManager's reporter
    # writes <path>.<uuid> here and the driver globs them. MUST be shared, not node-local.
    # Driver (BenchmarkRunner) reads CLUSTERING_METRICS_FILE; reporters read the flink-conf
    # entry below. Reporter jar must be in $FLINK_HOME/lib (see README).
    export CLUSTERING_METRICS_FILE="{output_dir}/metrics/$RUN_ID/flink-metrics.txt"
    mkdir -p "$(dirname "$CLUSTERING_METRICS_FILE")"

    # Writable Flink conf dir on SHARED storage (so TaskManagers started via srun on OTHER
    # nodes read the same per-run config — a node-local /tmp conf is invisible to workers).
    export FLINK_CONF_DIR="{output_dir}/flink-conf/$RUN_ID"
    mkdir -p "$FLINK_CONF_DIR"
    cp -r "$FLINK_HOME"/conf/* "$FLINK_CONF_DIR"/ 2>/dev/null || true
}}

configure_cluster() {{
    JM_HOST=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)
    TMS_PER_NODE={tms_per_node}
    SLOTS_PER_TM={slots_per_tm}
    TOTAL_TMS=$(( SLURM_NNODES * TMS_PER_NODE ))
    TOTAL_SLOTS=$(( TOTAL_TMS * SLOTS_PER_TM ))
    FLINK_TMP_DIR="/tmp/flink_tmp_${{SLURM_JOB_ID}}_$RUN_ID"
}}

write_flink_conf() {{
    {{
      echo "jobmanager.rpc.address: $JM_HOST"
      echo "jobmanager.rpc.port: 6123"
      echo "jobmanager.bind-host: 0.0.0.0"
      # Cross-node TM reachability needs BOTH: bind-host 0.0.0.0 (listen on all interfaces)
      # AND taskmanager.host = the node's real hostname (advertised address), set per-node via
      # -D in the srun launch below. With only bind-host, TMs advertise localhost; with only
      # host, they bind to localhost. Both together = advertise ac0xxx, listen on 0.0.0.0.
      echo "taskmanager.bind-host: 0.0.0.0"
      echo "rest.address: $JM_HOST"
      echo "rest.bind-address: 0.0.0.0"
      echo "rest.port: 8081"
      echo "taskmanager.numberOfTaskSlots: $SLOTS_PER_TM"
      echo "taskmanager.memory.process.size: {tm_mem}g"
      echo "jobmanager.memory.process.size: {jm_mem}g"
      echo "parallelism.default: {parallelism}"
      echo "io.tmp.dirs: $FLINK_TMP_DIR"
      echo "metrics.reporter.file.factory.class: clustering.metrics.FileMetricReporterFactory"
      echo "metrics.reporter.file.path: $CLUSTERING_METRICS_FILE"
      echo "metrics.reporter.file.interval: 1 SECONDS"
    }} >> "$FLINK_CONF_DIR/flink-conf.yaml"
}}

print_summary() {{
    echo "JobManager host: $JM_HOST   TaskManagers: $TOTAL_TMS   slots/TM: $SLOTS_PER_TM   total slots: $TOTAL_SLOTS   parallelism: {parallelism}"
}}

cleanup() {{
    EXIT_CODE=$?
    echo ">>> cleaning up $RUN_ID (exit=$EXIT_CODE)"
    "$FLINK_HOME/bin/taskmanager.sh" stop-all 2>/dev/null || true
    "$FLINK_HOME/bin/jobmanager.sh" stop 2>/dev/null || true
    rm -rf "$FLINK_TMP_DIR" "$FLINK_CONF_DIR"
    exit $EXIT_CODE
}}

start_jobmanager() {{
    "$FLINK_HOME/bin/jobmanager.sh" start
}}

start_taskmanagers() {{
    # One TaskManager per task across the allocation. Force taskmanager.host to each node's
    # own hostname ($(hostname) evaluated per-node inside srun): Flink's auto-detect picks
    # loopback here, so without this TMs advertise localhost and cross-node shuffle connects
    # to 127.0.0.1 and fails. The hostname resolves to the routable 172.22.x IP.
    srun --ntasks-per-node="$TMS_PER_NODE" \
         --cpus-per-task="$SLOTS_PER_TM" \
         --export=ALL \
         bash -c '"$FLINK_HOME/bin/taskmanager.sh" start-foreground -D taskmanager.host=$(hostname)' &
}}

wait_for_taskmanagers() {{
    echo -n "Waiting for $TOTAL_TMS TaskManagers"
    for _i in $(seq 1 60); do
        REGISTERED=$(curl -sf "http://$JM_HOST:8081/overview" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" 2>/dev/null || echo 0)
        if [ "$REGISTERED" -ge "$TOTAL_TMS" ]; then
            echo " OK ($REGISTERED in ${{_i}}s)"
            break
        fi
        echo -n "."
        sleep 2
    done
}}

submit_job() {{
    # bare `flink` is NOT on PATH — no Flink module; use the install's binary.
    "$FLINK_HOME/bin/flink" run \
      -m "$JM_HOST:8081" \
      -p "{parallelism}" \
      "{jar_path}" --config "$CONFIG_PATH"
}}

echo "=== RUN: $RUN_ID  (Job=$SLURM_JOB_ID) ==="
check_config
setup_dirs
configure_cluster
write_flink_conf
trap cleanup EXIT INT TERM
print_summary
start_jobmanager
start_taskmanagers
wait_for_taskmanagers
submit_job
"""
