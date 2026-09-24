# Dynamic Node Manager

## Table of contents

- [Dynamic Node Manager](#dynamic-node-manager)
  - [What it does](#what-it-does)
    - [1) Watches multiple namespaces](#1-watches-multiple-namespaces)
    - [2) Computes demand per namespace + queue](#2-computes-demand-per-namespace--queue)
    - [3) Triggers conversions when needed](#3-triggers-conversions-when-needed)
    - [4) Converts nodes from Slurm--Kubernetes](#4-converts-nodes-from-slurmkubernetes)
    - [5) Reverts nodes from Kubernetes--Slurm when idle](#5-reverts-nodes-from-kubernetes--slurm-when-idle)
  - [Key concepts](#key-concepts)
    - [YuniKorn queue capacity management](#yunikorn-queue-capacity-management)
    - [Pending pods with missing requests](#pending-pods-with-missing-requests)
    - [Bin-pack deadlock detection](#bin-pack-deadlock-detection)
  - [Architecture](#architecture)
    - [Evaluate loop (`evaluate_loop`)](#evaluate-loop-evaluate_loop)
    - [Actuator loop (`actuator_loop`)](#actuator-loop-actuator_loop)
  - [Files and dependencies](#files-and-dependencies)
    - [Python dependencies](#python-dependencies)
    - [Required files on disk](#required-files-on-disk)
    - [External command required](#external-command-required)
  - [Configuration](#configuration)
    - [`/etc/dynamic-node/dynamic_node_config.ini`](#etcdynamic-nodedynamic_node_configini)
    - [`/etc/dynamic-node/namespace_queue_paths.json`](#etcdynamic-nodenamespace_queue_pathsjson)
  - [Usage](#usage)
    - [Prerequisites](#prerequisites)
    - [Run in monitor mode](#run-in-monitor-mode)
    - [Run in test mode (generate queue load)](#run-in-test-mode-generate-queue-load)
    - [Common operational checks](#common-operational-checks)

## Overview

A Python service that watches Kubernetes namespaces, estimates YuniKorn queue demand (running + pending pods), and dynamically converts Slurm batch nodes into Kubernetes nodes when demand exceeds configured queue headroom or when bin-pack deadlocks are detected. When nodes go idle long enough, it reverts them back to Slurm and reduces YuniKorn queue capacity accordingly.

This tool is designed for environments where:
- Slurm/batch nodes can be switched into a Kubernetes pool via an external `node-convert` script.
- YuniKorn is used for queue management and its capacities are controlled through a `queues.yaml` stored in a ConfigMap.

---

## What it does

### 1) Watches multiple namespaces
You configure a set of `monitored_namespaces`. For each namespace, you also configure a corresponding YuniKorn `queue_path`.

### 2) Computes demand per namespace + queue
For each namespace’s queue path, it:
- Lists pods in that namespace.
- Filters pods to those assigned to the queue path (by annotation/label).
- Sums effective CPU/memory for:
  - **Running** pods (counted as “used”)
  - **Pending** pods (counted as “pending demand”)
- Applies default requests for Pending pods that declare no resources (so “invisible” pending work still triggers scaling).

### 3) Triggers conversions when needed
Conversion is triggered when any of these is true:
- Total demand (running + pending) exceeds queue headroom for CPU or memory.
- Pending demand exceeds available capacity (after subtracting running usage).
- A “bin-pack blocked” condition is detected (see below).

### 4) Converts nodes from Slurm → Kubernetes
When scaling out is required, the actuator:
- Runs `/usr/site/rcac/sbin/node-convert --set k8s ... --namespace <ns>`
- Waits for new node(s) to appear in the Kubernetes API
- Records converted nodes in a local JSON file
- Increases YuniKorn queue capacity by `(nodes_added * node_cpu_capacity, nodes_added * node_memory_capacity)` **only after** the nodes actually join the cluster

### 5) Reverts nodes from Kubernetes → Slurm when idle
Periodically checks converted nodes:
- If a converted node has **no active pods** in its namespace, and it has been idle longer than `reversion_idle_seconds`, the tool:
  - runs `/usr/site/rcac/sbin/node-convert --set batch --node-name <node>`
  - waits for node to disappear from the K8s API
  - decreases YuniKorn queue capacity accordingly
  - removes it from the converted node registry

---

## Key concepts

### YuniKorn queue capacity management
This tool treats YuniKorn as the source of truth for “how much capacity a queue is allowed to consume”.

It:
- Reads `queues.yaml (ConfigMap: yunikorn-configs by default)`
- Locates the queue by `queue_path` (dot-separated path like `root.platform.services.dev`)
- Adjusts:
  - `resources.max.{vcore,memory}`
  - `resources.guaranteed.{vcore,memory}`
- Ensures `guaranteed <= max` after changes

### Pending pods with missing requests
If a pod is `Pending` and has no declared requests/limits, this tool assigns defaults:
- CPU: `default_cpu_request_m` (milliCPU)
- Memory: `default_mem_request` (parsed via `kubernetes.utils.quantity.parse_quantity`)

This is critical to avoid false “no demand” conditions.

### Bin-pack deadlock detection
A common failure mode: you have available queue headroom on paper, but the cluster cannot schedule the next pending pod because it needs a fresh node (fragmentation/binpack).

This tool estimates:
- `max_pend_cpu`, `max_pend_mem`: largest pending pod “shape”
- `pods_per_node`: how many of that shape fit on a node
- `running_pods_est`: rough estimate of how many pods are already consuming the queue

If it detects a “perfectly filled” pattern with pending work waiting, it triggers conversion for at least one node.

---

## Architecture

The service runs two daemon threads:

### Evaluate loop (`evaluate_loop`)
Runs every `check_interval_seconds`:
1. `trigger()`  
   Computes needs per namespace and enqueues conversion tasks into a global queue.
2. `check_converted_nodes()`  
   Finds idle converted nodes and reverts them.

### Actuator loop (`actuator_loop`)
Continuously:
- Checks global conversion cap
- Enforces cooldown
- Pops tasks from the global queue
- Performs node conversion (1 node per task)
- Updates YuniKorn capacity after nodes join

---

## Files and dependencies

### Python dependencies
- `kubernetes` Python client
- `pyyaml`

### Required files on disk
- `/etc/dynamic-node/dynamic_node_config.ini`  
  Main configuration.
- `/etc/dynamic-node/namespace_queue_paths.json`  
  Map of namespace → YuniKorn queue path.

### External command required
- `/usr/site/rcac/sbin/node-convert`  
  Used to convert nodes between Slurm batch and Kubernetes.

#### Checking viability without converting

```
node-convert --list                     # every Slurm node
node-convert --list --node-type a       # only 'a' nodes
node-convert --list --node-name a001    # one node
```

Read-only: it runs the exact same eligibility logic the conversion path uses and
prints a verdict per node, so you can see *why* a node was rejected.

```
NODE             STATE                            IDLE  VERDICT REASON
a001             IDLE                           12h40m  OK      -
a008             ALLOCATED                          0s  PREEMPT job 6000001 (x-ob/pur230002) running 1h0m
a009             ALLOCATED                          0s  PREEMPT job 6000001 (x-ob/pur230002) running 1h0m
a006             IDLE                            4d14h  SKIP    reservation maint_sep
a007             IDLE                            2d14h  SKIP    planned for pending job 5510003
a003             IDLE+DRAIN                       1d5h  SKIP    state flag DRAIN
a002             IDLE+PLANNED                    5h40m  SKIP    state flag PLANNED
a004             ALLOCATED                         40m  SKIP    not idle (ALLOCATED)
a005             IDLE                              30s  SKIP    idle only 30s (< 600s)
a010             ALLOCATED                          0s  SKIP    not idle (ALLOCATED)

Viable now: 1 of 10 node(s) matching prefix 'a'
Preemptible: 2 node(s) held by x-ob/pur230002
```

`OK` = usable right now. `PREEMPT` = busy, but the job holding it is preemptible
backfill. `SKIP` = off limits, with the reason.

#### Preempting backfill jobs when nothing is idle

When fewer nodes are viable than requested, `--allow-preempt` lets `node-convert`
free some by preempting jobs that match *both* `PREEMPT_USER` and
`PREEMPT_ACCOUNT` (default `x-ob` / `pur230002`). Nothing else is ever touched,
and preemption is off unless explicitly enabled.

These jobs are single-node by construction, and the script enforces that rather
than assuming it: a matching job spanning more than one node is reported and
skipped, never preempted. Killing a 4-node job to satisfy a 1-node request would
destroy four nodes' worth of work. `--list` names the job and its node count so
the skip is visible.

```
node-convert --set k8s --node-type a --num-nodes 2 --allow-preempt
```

Order of operations, which matters:

1. Pick the **youngest** matching jobs first, so the least compute is lost.
   `PREEMPT_MAX_RUNTIME_SECONDS` additionally protects jobs past a given age.
2. Skip jobs that span more than one node or whose node is outside the requested
   type, and ping-check the node *before* destroying any work.
3. Add the nodes to the `k8s` reservation **first** (with `IGNORE_JOBS`, which
   allows reserving a busy node). Without this, the scheduler can hand the node
   to another job in the gap between the job exiting and `k8s-convert` running.
4. `SIGTERM` the job, wait `PREEMPT_GRACE_SECONDS`, then requeue it
   (`PREEMPT_MODE=requeue`, the default) or cancel it (`PREEMPT_MODE=cancel`).
   A job with `Requeue=0` is skipped rather than killed, unless mode is `cancel`.
5. Wait for the node to leave `COMPLETING` and reach `IDLE`, then convert.
6. If the conversion aborts anyway (too few reachable nodes), release the
   preempted nodes from the reservation. Otherwise they sit reserved and
   unschedulable with nothing running on them.

If `PROTECT_LARGEST_PENDING=1` trips, no preemption is attempted either: a
starved large job should not be made worse.

Consider whether Slurm's native preemption (a preemptible QOS or partition plus
`PreemptMode=REQUEUE`) fits better. It is the supported mechanism and it lets the
scheduler make the choice; this flag exists for the case where those jobs are a
convention rather than a configured QOS.

#### Node eligibility (batch → k8s)

A Slurm node is only taken if **all** of these hold:

- base state is `IDLE` with no `PLANNED`, `RESERVED`, `DRAIN*`, `DOWN`, `FAIL*`,
  `MAINT`, `REBOOT_*`, `POWER*`, `NOT_RESPONDING`, `INVALID_REG` or `COMPLETING` flag
- `LastBusyTime` is at least `MIN_IDLE_SECONDS` ago
- the node is not listed in `SchedNodes` of any pending job (i.e. the backfill
  scheduler has not already earmarked it for a job that is waiting to start)
- the node is not in a reservation that is active or starts within
  `RES_LOOKAHEAD_SECONDS` (the `k8s` reservation itself is excluded)

Candidates are sorted longest-idle first. After the node joins the `k8s`
reservation its state is re-checked, and reservation membership is rolled back
if Slurm allocated it in the meantime.

Environment overrides:

- `MIN_IDLE_SECONDS` (default `600`)
- `RES_LOOKAHEAD_SECONDS` (default `86400`)
- `PROTECT_LARGEST_PENDING` (default `0`, disabled) — when set to `1`, refuse to
  convert if doing so would leave fewer eligible idle nodes than the largest
  pending job requests.
- `PREEMPT_ENABLED` (default `0`) — same as passing `--allow-preempt`.
- `PREEMPT_USER` / `PREEMPT_ACCOUNT` (default `x-ob` / `pur230002`) — a job must
  match both to be preemptible.
- `PREEMPT_MODE` (default `requeue`) — `requeue` or `cancel`.
- `PREEMPT_GRACE_SECONDS` (default `60`) — `SIGTERM` first, then wait this long.
- `PREEMPT_MAX_RUNTIME_SECONDS` (default `0`, no limit) — never preempt a job
  that has run longer than this.
- `PREEMPT_DRAIN_TIMEOUT` (default `300`) — how long to wait for a preempted
  node to reach `IDLE`.
- `PASSWORD` (no default, **required for `--set batch`**) — the racadm password
  used to power cycle a node on its way back to Slurm. `--set batch` refuses to
  start without it, before anything is drained.

Note: the `PLANNED` node state requires Slurm 22.05 or newer; on older versions
the `SchedNodes` check carries most of the weight.

### Failure handling

Every external command (`scontrol`, `squeue`, `kubectl`, `pdsh`, `ssh`/racadm)
is checked, and the script is built so that a failure never leaves a node in
neither pool:

- If Slurm or Kubernetes cannot be queried, nothing is converted. A node is
  never judged viable on the strength of a query that failed — an unreadable
  pending queue would otherwise make backfill-planned nodes look idle.
- The `k8s` reservation is never rewritten from a membership list that could not
  be read, and it is never deleted just because reading it failed.
- If `pdsh` fails part way through, the affected nodes stay drained and in the
  reservation (and so out of Slurm's reach) and the script names them, rather
  than handing half-converted machines back to the batch scheduler.
- Selected nodes are drained with a `Reason` (`MARK_REASON`, default `batch node
  selected for k8s conversion`) so `sinfo -R` explains why they left the batch
  pool — a reservation carries no per-node note of its own, and Slurm only keeps
  a `Reason` on a node that is out of service. Any rollback that releases the
  reservation also clears the drain — otherwise the node would be neither
  reserved nor schedulable, and `DRAIN` would keep it out of future selection
  too.
- On the way back to batch, each step gates the next: a node that could not be
  drained is not deleted, and a node that could not be power cycled is not
  resumed in Slurm and keeps its reservation. In a multi-node revert the nodes
  that succeeded are released normally and the failures are listed; the exit
  status is non-zero if any node failed.

---

## Configuration

### `/etc/dynamic-node/dynamic_node_config.ini`

Expected keys (section `[settings]`):

- `kubeconfig_path` (optional)  
  If set, loads kubeconfig from this path. Otherwise uses default kubeconfig loading behavior.
- `monitored_namespaces`  
  Comma-separated namespaces to watch.
- `reversion_idle_seconds`  
  How long a converted node must be idle (no active pods) before revert.
- `check_interval_seconds`  
  Evaluate loop interval.
- `max_converted_nodes`  
  Global cap on how many nodes may be converted at any time.
- `allow_preempt` (default `false`)  
  Passes `--allow-preempt` to `node-convert`, letting it preempt single-node
  backfill jobs when no node is idle. See the `node-convert` section above.
- `yunikorn_cm_namespace` (default: `yunikorn`)  
  Namespace containing the YuniKorn ConfigMap.
- `yunikorn_cm_name` (default: `yunikorn-configs`)  
  Name of the YuniKorn ConfigMap containing `queues.yaml`.
- `yunikorn_queue_annotation` (default: `yunikorn.apache.org/queue`)  
  Key used to identify queue assignment on pods (annotation or label).
- `node_cpu_capacity` (default: `128`)  
  vcores to add/subtract per converted node.
- `node_memory_capacity` (default: `256G`)  
  memory to add/subtract per converted node.
- `global_cooldown_seconds` (default: `30`)  
  Cooldown between conversions (note: present but not currently wired to advance `_next_conversion_allowed_at`).
- `default_cpu_request_m` (default: `1000`)  
  Used only when pending pods have no resource requests/limits.
- `default_mem_request` (default: `2Gi`)  
  Used only when pending pods have no resource requests/limits.

### `/etc/dynamic-node/namespace_queue_paths.json`

A JSON object mapping each monitored namespace to a YuniKorn queue path:

```json
{
  "workloads-dev": "root.platform.services.dev",
  "workloads-prod": "root.platform.services.prod"
}
```

## Usage

This service supports two modes:

- **monitor**: run the continuous evaluate + actuator loops (normal operation)
- **test**: create a synthetic load Deployment in a namespace/queue (validation)

### Prerequisites

- A valid kubeconfig accessible to the process (either:
  - set `kubeconfig_path` in `/etc/dynamic-node/dynamic_node_config.ini`, or
  - rely on default kubeconfig loading via `KUBECONFIG` / `~/.kube/config`)
- `/etc/dynamic-node/dynamic_node_config.ini` is present and readable
- `/etc/dynamic-node/namespace_queue_paths.json` is present and contains queue mappings for all monitored namespaces
- The process identity can:
  - list nodes and pods
  - read/patch the YuniKorn ConfigMap containing `queues.yaml`
  - (test mode) create/patch/delete Deployments in the target namespace
- The external converter exists and is executable:
  - `/usr/site/rcac/sbin/node-convert`

### Run in monitor mode

Monitor mode starts two background threads:
- **Evaluate loop**: periodically computes demand and enqueues conversions; checks for idle nodes to revert
- **Actuator loop**: drains the conversion queue and performs `node-convert` actions

```bash
python3 dynamic-node-manager.py --mode monitor
```
### Run in test mode (generate queue load)

Test mode creates a Deployment that schedules pods annotated to a specific YuniKorn queue. Each pod runs a long sleep to hold resources.

```bash
python3 dynamic-node-manager.py \
  --mode test \
  --namespace workloads-dev \
  --queue root.platform.services.dev \
  --replicas 200
```

### Common operational checks

- Verify the manager is logging demand decisions and conversion actions via syslog.
- Confirm YuniKorn `queues.yaml` is being updated (capacity changes show as `MAX`/`GUAR` bumps in logs).
- Confirm nodes join/leave the Kubernetes API after `node-convert` runs.
- Confirm `/etc/dynamic-node/converted_nodes.json` is being updated as nodes are converted and reverted.

