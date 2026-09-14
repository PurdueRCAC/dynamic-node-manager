import os
import argparse
import time
import subprocess
import configparser
import syslog
from datetime import datetime
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
from kubernetes.utils.quantity import parse_quantity
import json
import yaml
from kubernetes.client import V1ObjectMeta, V1Deployment, V1DeploymentSpec, V1LabelSelector
from kubernetes.client import V1PodTemplateSpec, V1PodSpec, V1Container, V1ResourceRequirements
from kubernetes.client import AppsV1Api
import random
import string
import math
from threading import Thread, Lock


DEDICATED_NS_LABEL = "dynamic.dedicated"
NAMESPACE_TAINT_KEY = "dynamic.dedicated"
DEDICATED_MARK_TAINT_KEY = "dedicated"
DEDICATED_MARK_TAINT_VALUE = "batch"
DEDICATED_MARK_TAINT_EFFECT = "NoSchedule"

NAMESPACE_MARK_TAINT_EFFECT = "NoSchedule"
PENDING_CONVERSIONS = []
PENDING_LOCK = Lock()


class SysLogger:
    """
    Thin wrapper around syslog so the rest of the code can log using .info/.warning/.error
    with printf-style formatting.
    """

    # Initializes syslog with an ident and facility.
    def __init__(self, ident="dynamic-node-manager", facility=syslog.LOG_DAEMON):
        syslog.openlog(ident=ident, logoption=syslog.LOG_PID, facility=facility)

    # Formats a message safely, falling back to string-joining if %-formatting fails.
    def _fmt(self, msg, *args):
        try:
            return msg % args if args else str(msg)
        except Exception:
            # fallback: join if formatting fails
            return " ".join([str(msg)] + [str(a) for a in args])

    # Logs an informational message to syslog.
    def info(self, msg, *args):
        syslog.syslog(syslog.LOG_INFO, self._fmt(msg, *args))

    # Logs a warning message to syslog.
    def warning(self, msg, *args):
        syslog.syslog(syslog.LOG_WARNING, self._fmt(msg, *args))

    # Logs an error message to syslog.
    def error(self, msg, *args):
        syslog.syslog(syslog.LOG_ERR, self._fmt(msg, *args))


logger = SysLogger()


# Loads the INI configuration file from disk and returns a ConfigParser instance.
def load_config(path="/etc/dynamic-node/dynamic_node_config.ini"):
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


class DynamicNodeManager:
    """
    Main service class. On startup it:
      - loads config.ini and namespace->queue mappings
      - loads kubeconfig and creates Kubernetes API clients
      - loads/maintains a registry of converted nodes
    It then runs:
      - an evaluate loop (compute demand, enqueue conversions, revert idle nodes)
      - an actuator loop (perform conversions one task at a time)
    """

    # Initializes the manager: loads config, kubeconfig, API clients, and runtime settings.
    def __init__(self):
        config_data = load_config()

        kubeconfig_path = config_data.get("settings", "kubeconfig_path", fallback=None)
        try:
            if kubeconfig_path:
                config.load_kube_config(config_file=kubeconfig_path)
                logger.info(f"Loaded kubeconfig from {kubeconfig_path}")
            else:
                config.load_kube_config()
                default = os.environ.get("KUBECONFIG", "~/.kube/config")
                logger.info(f"Loaded default kubeconfig from {default}")
        except Exception as e:
            logger.error(f"Failed to load kubeconfig: {e}")
            raise

        self.v1 = client.CoreV1Api()
        self.rq = client.CoreV1Api()
        self.monitored_namespaces = [
            s.strip()
            for s in config_data.get("settings", "monitored_namespaces").split(",")
        ]
        self.reversion_idle_seconds = int(
            config_data.get("settings", "reversion_idle_seconds")
        )
        self.check_interval_seconds = int(
            config_data.get("settings", "check_interval_seconds")
        )
        self.max_converted_nodes = int(config_data.get("settings", "max_converted_nodes"))
        self.cm_ns = config_data.get(
            "settings", "yunikorn_cm_namespace", fallback="yunikorn"
        )
        self.cm_name = config_data.get(
            "settings", "yunikorn_cm_name", fallback="yunikorn-configs"
        )
        self.namespace_queue_paths = json.load(
            open("/etc/dynamic-node/namespace_queue_paths.json", "r")
        )
        self.yk_queue_annotation = config_data.get(
            "settings",
            "yunikorn_queue_annotation",
            fallback="yunikorn.apache.org/queue",
        )
        self.converted_nodes_path = "/etc/dynamic-node/converted_nodes.json"
        self.converted_nodes = self.load_converted_nodes()
        self.node_vcores = int(config_data.get("settings", "node_cpu_capacity", fallback="128"))
        self.node_mem_bytes = self._parse_mem(
            config_data.get("settings", "node_memory_capacity", fallback="256G")
        )
        self.global_cooldown_seconds = int(
            config_data.get("settings", "global_cooldown_seconds", fallback="30")
        )
        self._next_conversion_allowed_at = 0
        self.default_cpu_request_m = int(
            config_data.get("settings", "default_cpu_request_m", fallback="1000")
        )
        self.default_mem_request = str(
            config_data.get("settings", "default_mem_request", fallback="2Gi")
        )
        self.default_mem_request_bytes = self.parse_mem_quantity(self.default_mem_request)
        # When set, node-convert may preempt single-node backfill jobs (see its
        # PREEMPT_* settings) if no node is idle. Off unless explicitly enabled.
        self.allow_preempt = str(
            config_data.get("settings", "allow_preempt", fallback="false")
        ).strip().lower() in ("1", "true", "yes", "on")
        if self.allow_preempt:
            logger.warning(
                "allow_preempt is enabled: conversions may preempt running batch jobs"
            )

    # Scans live Kubernetes nodes and counts which ones look "converted" into a namespace pool.
    # Returns: (total_converted, per_namespace_breakdown).
    def get_converted_counts(self):
        total = 0
        per_ns = {}
        try:
            for n in self.v1.list_node().items:
                labels = n.metadata.labels or {}
                taints = (getattr(n.spec, "taints", None) or [])
                node_name = getattr(n.metadata, "name", "<unknown>")

                matched_namespaces = []
                match_reasons = {}

                for ns in self.monitored_namespaces:
                    cond_label_namespace = (str(labels.get("namespace", "")).strip() == ns)
                    cond_label_boolean = (str(labels.get(ns, "")).strip().lower() == "true")
                    cond_taint_boolean = any(
                        getattr(t, "key", None) == ns and
                        str(getattr(t, "value", "")).strip().lower() == "true" and
                        getattr(t, "effect", None) == NAMESPACE_MARK_TAINT_EFFECT
                        for t in taints
                    )

                    if cond_label_namespace or cond_label_boolean or cond_taint_boolean:
                        matched_namespaces.append(ns)
                        reasons = []
                        if cond_label_namespace:
                            reasons.append(f'namespace label matches (namespace={labels.get("namespace")})')
                        if cond_label_boolean:
                            reasons.append(f'boolean label present ({ns}=true)')
                        if cond_taint_boolean:
                            reasons.append(f'taint present ({ns}=true/{NAMESPACE_MARK_TAINT_EFFECT})')
                        match_reasons[ns] = "; ".join(reasons) if reasons else "matched"

                if matched_namespaces:
                    total += 1
                    if len(matched_namespaces) == 1:
                        ns = matched_namespaces[0]
                        if ns not in per_ns:
                            per_ns[ns] = {"count": 0, "nodes": []}
                        per_ns[ns]["count"] += 1
                        per_ns[ns]["nodes"].append(node_name)
                    else:
                        per_ns.setdefault("multiple", {"count": 0, "details": []})
                        per_ns["multiple"]["count"] += 1
                        per_ns["multiple"]["details"].append({
                            "node": node_name,
                            "namespaces": matched_namespaces
                        })

        except ApiException as e:
            logger.error("Failed to list nodes for converted count: %s", e)

        return total, per_ns

    # Loads the on-disk registry of converted nodes (node_name -> metadata).
    def load_converted_nodes(self):
        if os.path.exists(self.converted_nodes_path):
            with open(self.converted_nodes_path, "r") as f:
                return json.load(f)
        return {}

    # Persists the in-memory converted node registry to disk.
    def save_converted_nodes(self):
        os.makedirs(os.path.dirname(self.converted_nodes_path), exist_ok=True)
        with open(self.converted_nodes_path, "w") as f:
            json.dump(self.converted_nodes, f, default=str)

    # Parses Kubernetes quantity strings (e.g., "256Gi", "256G") into bytes (int).
    def _parse_mem(self, quantity: str) -> int:
        try:
            return int(parse_quantity(str(quantity)))
        except Exception as e:
            logger.error(f"Failed to parse memory '{quantity}': {e}")
            return 0

    # Formats a byte value back into a human string using the suffix style of like_str.
    def _format_mem_like(self, bytes_val, like_str):
        suffix = None
        if isinstance(like_str, str):
            s = like_str.strip()
            if s.endswith("Gi"):
                suffix = "Gi"
            elif s.endswith("G"):
                suffix = "G"
            elif s.endswith("Mi"):
                suffix = "Mi"
            elif s.endswith("M"):
                suffix = "M"

        if suffix == "Gi":
            val = bytes_val / (1024**3)
            return f"{int(round(val))}Gi"
        elif suffix == "Mi":
            val = bytes_val / (1024**2)
            return f"{int(round(val))}Mi"
        elif suffix == "M":
            val = bytes_val / 1_000_000
            return f"{int(round(val))}M"
        else:
            val = bytes_val / 1_000_000_000
            return f"{int(round(val))}G"

    # Reads the YuniKorn ConfigMap and parses queues.yaml into a Python object.
    def _get_queues_yaml(self):
        cm = self.v1.read_namespaced_config_map(self.cm_name, self.cm_ns)
        text = (cm.data or {}).get("queues.yaml", "")
        if not text:
            raise RuntimeError("queues.yaml missing from ConfigMap")
        return yaml.safe_load(text), cm

    # Walks the queues.yaml structure to find the queue dict corresponding to queue_path.
    def _find_queue(self, queues_obj, queue_path: str):
        parts = queue_path.split(".")
        if parts[0] != "root":
            raise ValueError("Queue path must start with 'root'")
        level = queues_obj["partitions"][0]["queues"]
        q = None
        for name in parts[1:]:
            q = next((item for item in level if item.get("name") == name), None)
            if not q:
                raise KeyError(f"Queue segment '{name}' not found in path {queue_path}")
            level = q.get("queues", [])
        return q

    # Parses CPU quantities like "1000m" or "2" into a float vcore value.
    def parse_cpu_quantity(self, quantity):
        try:
            return float(parse_quantity(str(quantity)))
        except Exception as e:
            logger.error(f"Failed to parse quantity '{quantity}': {e}")
            return 0.0

    # Parses memory quantities like "2Gi" into a float byte value.
    def parse_mem_quantity(self, quantity):
        try:
            return float(parse_quantity(quantity))
        except Exception as e:
            logger.error(f"Failed to parse memory '{quantity}': {e}")
            return 0.0

    # Helper to ensure nested dict paths exist, returning the final dict at the leaf key.
    def _get_set(self, dct, *keys, default_factory=dict):
        cur = dct
        for k in keys[:-1]:
            cur = cur.setdefault(k, {})
        return cur.setdefault(keys[-1], default_factory())

    # Applies a +/- capacity adjustment to YuniKorn queue max/guaranteed based on node count.
    # Ensures guaranteed <= max, then patches the ConfigMap.
    def _update_queue_capacity(self, queue_path: str, nodes: int):
        """
        Adjust queue capacity by <nodes> * (node_vcores, node_mem_bytes).
        Positive nodes => increase; negative => decrease.
        """
        queues_obj, cm = self._get_queues_yaml()
        q = self._find_queue(queues_obj, queue_path)

        v_delta = self.node_vcores * int(nodes)
        m_delta_bytes = self.node_mem_bytes * int(nodes)

        res = self._get_set(q, "resources", default_factory=dict)
        mx = self._get_set(res, "max", default_factory=dict)
        gr = self._get_set(res, "guaranteed", default_factory=dict)

        def bump(section: dict):
            old_v = int(str(section.get("vcore", "0")) or 0)
            new_v = max(0, old_v + v_delta)
            old_m_str = str(section.get("memory", "0G"))
            old_m_bytes = self._parse_mem(old_m_str)
            new_m_bytes = max(0, old_m_bytes + m_delta_bytes)
            new_m_str = self._format_mem_like(new_m_bytes, old_m_str)

            section["vcore"] = new_v
            section["memory"] = new_m_str
            return old_v, new_v, old_m_str, new_m_str, new_m_bytes

        old_v_max, new_v_max, old_m_max_str, new_m_max_str, new_m_max_bytes = bump(mx)
        old_v_g, new_v_g, old_m_g_str, new_m_g_str, new_m_g_bytes = bump(gr)

        if new_v_g > new_v_max:
            gr["vcore"] = new_v_max
        if new_m_g_bytes > new_m_max_bytes:
            gr["memory"] = self._format_mem_like(new_m_max_bytes, old_m_g_str)

        new_text = yaml.safe_dump(queues_obj, sort_keys=False)
        body = {"data": {"queues.yaml": new_text}}
        self.v1.patch_namespaced_config_map(self.cm_name, self.cm_ns, body)

        logger.info(
            "Updated YuniKorn %s/%s for %s by %d node(s): "
            "MAX vcore %s->%s, memory %s->%s; "
            "GUAR vcore %s->%s, memory %s->%s",
            self.cm_ns,
            self.cm_name,
            queue_path,
            nodes,
            old_v_max,
            new_v_max,
            old_m_max_str,
            new_m_max_str,
            old_v_g,
            gr["vcore"],
            old_m_g_str,
            gr["memory"],
        )

    # Public helper to increase queue capacity by N nodes.
    def increase_queue_capacity(self, queue_path: str, nodes: int = 1):
        self._update_queue_capacity(queue_path, nodes)

    # Public helper to decrease queue capacity by N nodes.
    def decrease_queue_capacity(self, queue_path: str, nodes: int = 1):
        self._update_queue_capacity(queue_path, -nodes)

    # Reads YuniKorn queue max vcores/memory from queues.yaml for a given queue_path.
    def get_queue_max_caps(self, queue_path: str):
        qyaml, _ = self._get_queues_yaml()
        q = self._find_queue(qyaml, queue_path)
        maxr = (q.get("resources") or {}).get("max") or {}
        max_vcores = int(str(maxr.get("vcore", "0")))
        max_mem = self.parse_mem_quantity(str(maxr.get("memory", "0")))
        return max_vcores, max_mem

    # Computes used + pending demand for a namespace/queue by inspecting pods:
    #   - filters pods to those assigned to the queue
    #   - sums effective cpu/mem for Running and Pending
    #   - applies defaults for Pending pods with missing resource requests
    # Also returns the largest pending pod "shape" for binpack-deadlock detection.
    def get_queue_usage_and_pending(self, namespace: str, queue_path: str):
        pods = self.v1.list_namespaced_pod(namespace).items
        used_v, used_m = 0.0, 0.0
        pend_v, pend_m = 0.0, 0.0
        pend_count = 0
        max_pend_cpu = 0.0
        max_pend_mem = 0.0

        qkey = self.yk_queue_annotation
        qval = (queue_path or "").strip()

        for pod in pods:
            ann_map = pod.metadata.annotations or {}
            lab_map = pod.metadata.labels or {}
            ann = ((ann_map.get(qkey) or lab_map.get(qkey) or "")).strip()

            if ann != qval:
                logger.info(
                    "Skipping pod %s/%s: queue key=%s ann=%r label=%r, expected=%r",
                    namespace,
                    getattr(pod.metadata, "name", "<unnamed>"),
                    qkey,
                    ann_map.get(qkey, ""),
                    lab_map.get(qkey, ""),
                    qval,
                )
                continue

            def sum_resources(conts):
                rv, rm = 0.0, 0.0
                for c in conts or []:
                    req = c.resources.requests or {}
                    lim = c.resources.limits or {}
                    cpu_src = req.get("cpu", lim.get("cpu"))
                    mem_src = req.get("memory", lim.get("memory"))
                    if cpu_src:
                        rv += self.parse_cpu_quantity(str(cpu_src))
                    if mem_src:
                        rm += self.parse_mem_quantity(str(mem_src))
                return rv, rm

            req_v, req_m = sum_resources(pod.spec.containers)
            init_v, init_m = sum_resources(getattr(pod.spec, "init_containers", None))
            eff_v = max(req_v, init_v)
            eff_m = max(req_m, init_m)

            phase = getattr(pod.status, "phase", "Unknown")

            if phase == "Pending" and eff_v == 0.0 and eff_m == 0.0:
                eff_v = float(self.default_cpu_request_m) / 1000.0
                eff_m = float(self.default_mem_request_bytes)
                logger.info(
                    "Pod %s/%s Pending with no requests/limits; applying defaults cpu=%sm mem=%s (%d bytes)",
                    namespace,
                    getattr(pod.metadata, "name", "<unnamed>"),
                    self.default_cpu_request_m,
                    self.default_mem_request,
                    int(self.default_mem_request_bytes),
                )

            if phase == "Pending":
                pend_v += eff_v
                pend_m += eff_m
                pend_count += 1
                max_pend_cpu = max(max_pend_cpu, eff_v)
                max_pend_mem = max(max_pend_mem, eff_m)

                logger.info(
                    "PENDING pod %s/%s contributes cpu=%.3f vcores, mem=%d bytes (eff from req/lim/init)",
                    namespace,
                    getattr(pod.metadata, "name", "<unnamed>"),
                    eff_v,
                    int(eff_m),
                )
            elif phase == "Running":
                used_v += eff_v
                used_m += eff_m
                logger.info(
                    "RUNNING pod %s/%s contributes cpu=%.3f vcores, mem=%d bytes (eff from req/lim/init)",
                    namespace,
                    getattr(pod.metadata, "name", "<unnamed>"),
                    eff_v,
                    int(eff_m),
                )
            else:
                logger.info(
                    "Ignoring pod %s/%s in phase=%s (no demand counted)",
                    namespace,
                    getattr(pod.metadata, "name", "<unnamed>"),
                    phase,
                )

        logger.info(
            "[%s|%s] usage summary: used_v=%.2f, used_m=%d, pend_v=%.2f, pend_m=%d, pend_count=%d, max_pend_cpu=%.2f, max_pend_mem=%d",
            namespace,
            queue_path,
            used_v,
            int(used_m),
            pend_v,
            int(pend_m),
            pend_count,
            max_pend_cpu,
            int(max_pend_mem),
        )
        return used_v, used_m, pend_v, pend_m, pend_count, max_pend_cpu, max_pend_mem

    # Adds a dedicated label + taints to a node so only the target namespace's workloads can land.
    def add_dedicated_taint_and_label(self, node_name, namespace):
        body = {
            "metadata": {"labels": {DEDICATED_NS_LABEL: namespace}},
            "spec": {
                "taints": [
                    {"key": NAMESPACE_TAINT_KEY, "value": namespace, "effect": "NoSchedule"},
                    {"key": DEDICATED_MARK_TAINT_KEY, "value": DEDICATED_MARK_TAINT_VALUE, "effect": DEDICATED_MARK_TAINT_EFFECT},
                ]
            },
        }
        self.v1.patch_node(node_name, body)

    # Converts nodes from batch -> k8s using node-convert, then waits for nodes to appear in the API.
    # Only after nodes are confirmed does it:
    #   - record them in converted_nodes.json
    #   - increase YuniKorn capacity for the queue
    def convert_node_to_k8s(self, namespace: str, queue_path: str):
        """Convert as many nodes as needed; only bump capacity after nodes appear."""
        import time

        HEADROOM = 0.95
        DISCOVERY_TIMEOUT = 180
        DISCOVERY_POLL = 2

        env = os.environ.copy()
        env["KUBECONFIG"] = os.environ.get("KUBECONFIG", os.path.expanduser("~/.kube/config"))

        try:
            before_nodes = {n.metadata.name for n in self.v1.list_node().items}
        except Exception as e:
            logger.error("Failed to list nodes before conversion: %s", e)
            before_nodes = set()

        need = self._needed_nodes_for_ns(namespace, queue_path, HEADROOM)
        if need <= 0:
            logger.info("convert_node_to_k8s: no deficit for ns=%s queue=%s; skipping", namespace, queue_path)
            return []

        total_converted, _ = self.get_converted_counts()
        remaining_overall = max(0, self.max_converted_nodes - total_converted)
        nodes_to_convert = min(need, remaining_overall)
        if nodes_to_convert <= 0:
            logger.info(
                "convert_node_to_k8s: global cap reached (max=%d, current=%d); skipping",
                self.max_converted_nodes,
                total_converted,
            )
            return []

        logger.info("convert_node_to_k8s: converting %d node(s) for ns=%s queue=%s", nodes_to_convert, namespace, queue_path)

        try:
            cmd = [
                "/usr/site/rcac/sbin/node-convert",
                "--set",
                "k8s",
                "--node-type",
                "a",
                "--num-nodes",
                str(nodes_to_convert),
                "--namespace",
                namespace,
            ]
            if self.allow_preempt:
                cmd.append("--allow-preempt")
            logger.info("Running: %s", " ".join(cmd))
            p = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
            )
            for line in p.stdout:
                logger.info("node-convert: %s", line.rstrip("\n"))
            rc = p.wait()
            if rc != 0:
                logger.error("node-convert failed rc=%s; aborting capacity change", rc)
                return []
        except subprocess.SubprocessError as e:
            logger.error("node-convert exception: %s", e)
            return []

        discovered = []
        deadline = time.time() + DISCOVERY_TIMEOUT
        while time.time() < deadline and len(discovered) < nodes_to_convert:
            try:
                current = {n.metadata.name for n in self.v1.list_node().items}
                diff = list(current - before_nodes)
                if diff:
                    discovered = diff[:nodes_to_convert]
                    if len(discovered) >= nodes_to_convert:
                        break
            except Exception as e:
                logger.warning("Failed to list nodes during discovery: %s", e)
            time.sleep(DISCOVERY_POLL)

        if not discovered:
            logger.error(
                "Conversion requested %d node(s) but none joined the cluster within %ds; no capacity change",
                nodes_to_convert,
                DISCOVERY_TIMEOUT,
            )
            return []

        for node_name in discovered:
            self.converted_nodes[node_name] = {
                "namespace": namespace,
                "last_pod_end": datetime.utcnow().isoformat(),
            }
        self.save_converted_nodes()

        try:
            self.increase_queue_capacity(queue_path, nodes=len(discovered))
            logger.info("Increased capacity for %s by %d node(s)", queue_path, len(discovered))
        except Exception as e:
            logger.error("Failed to update YuniKorn capacity for %s: %s", queue_path, e)

        logger.info("Converted %d node(s) for ns=%s queue=%s: %s", len(discovered), namespace, queue_path, ", ".join(discovered))
        return discovered

    # Reverts a specific node from k8s -> batch using node-convert, waits for it to disappear from the API,
    # then decreases YuniKorn capacity by one node.
    def revert_node_to_slurm(self, node_name: str, queue_path: str):
        logger.info(f"Reverting node {node_name} back to Slurm")

        DISCOVERY_TIMEOUT = 180
        DISCOVERY_POLL = 2

        env = os.environ.copy()
        env["KUBECONFIG"] = os.environ.get("KUBECONFIG", os.path.expanduser("~/.kube/config"))

        try:
            cmd = ["/usr/site/rcac/sbin/node-convert", "--set", "batch", "--node-name", node_name]
            logger.info("Running: %s", " ".join(cmd))

            p = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
            )
            for line in p.stdout:
                logger.info("node-convert(revert): %s", line.rstrip("\n"))
            rc = p.wait()
            if rc != 0:
                logger.error("node-convert revert script failed. rc=%s; not changing capacity", rc)
                return False
        except subprocess.SubprocessError as e:
            logger.error(f"Exception while running node-convert revert script: {e}")
            return False

        deadline = time.time() + DISCOVERY_TIMEOUT
        disappeared = False
        while time.time() < deadline:
            try:
                current_nodes = {n.metadata.name for n in self.v1.list_node().items}
                if node_name not in current_nodes:
                    disappeared = True
                    break
            except Exception as e:
                logger.warning("Error listing nodes while verifying revert of %s: %s", node_name, e)
            time.sleep(DISCOVERY_POLL)

        if not disappeared:
            logger.error("Node %s did not disappear from Kubernetes within %ds; not changing capacity", node_name, DISCOVERY_TIMEOUT)
            return False

        try:
            self.decrease_queue_capacity(queue_path, nodes=1)
            logger.info("Reduced capacity for %s by 1 node due to revert of %s", queue_path, node_name)
        except Exception as e:
            logger.error("Failed to reduce YuniKorn capacity for %s after revert of %s: %s", queue_path, node_name, e)
            return False

        return True

    # Checks all recorded converted nodes and reverts those that:
    #   - have no active pods in their recorded namespace
    #   - have been idle longer than reversion_idle_seconds
    # Also logs current converted-node state for observability.
    def check_converted_nodes(self):
        logger.info("Checking converted batch nodes for reversion eligibility...")

        if self.converted_nodes:
            logger.info("Currently converted batch nodes: %d total", len(self.converted_nodes))
            for node, info in self.converted_nodes.items():
                ns = info.get("namespace", "_unknown")
                queue_path = self.namespace_queue_paths.get(ns, "_unknown")
                logger.info("  - Node: %s | Namespace: %s | Queue: %s", node, ns, queue_path)
        else:
            logger.info(f"No batch nodes are currently converted. Checking again in {self.check_interval_seconds} seconds")

        now = datetime.utcnow()

        for node, info in list(self.converted_nodes.items()):
            ns = info.get("namespace")
            if not ns:
                logger.warning("Node %s has no recorded namespace; skipping namespace-scoped check.", node)
                continue

            try:
                pods = self.v1.list_namespaced_pod(namespace=ns, field_selector=f"spec.nodeName={node}").items
            except ApiException as e:
                logger.error("Failed to list pods for node %s in ns %s: %s", node, ns, e)
                continue

            active_pods = [p for p in pods if (p.status and p.status.phase not in ("Succeeded", "Failed"))]

            if active_pods:
                logger.info("Node %s has %d active pod(s) in ns=%s. Skipping reversion.", node, len(active_pods), ns)
                continue

            last_raw = info.get("last_pod_end")

            if last_raw in (None, "", 0):
                logger.info("Missing last_pod_end for %s; skipping reversion this pass.", node)
                continue

            try:
                if isinstance(last_raw, (int, float)):
                    last_dt = datetime.utcfromtimestamp(float(last_raw))
                else:
                    s = str(last_raw).rstrip("Z").split(".")[0]
                    last_dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                logger.warning("Bad last_pod_end for %s (%r); treating as now.", node, last_raw)
                last_dt = now

            idle = (now - last_dt).total_seconds()

            if idle < self.reversion_idle_seconds:
                logger.info("Node %s idle for %ss (< threshold). Skipping.", node, idle)
                continue

            queue_path = self.namespace_queue_paths.get(ns)
            if not queue_path:
                logger.warning("No queue path for namespace %s; cannot adjust capacity on revert.", ns)

            logger.info("Node %s idle for %ss (>= threshold). Reverting.", node, idle)
            if self.revert_node_to_slurm(node, queue_path or "root"):
                del self.converted_nodes[node]
                self.save_converted_nodes()
            else:
                logger.warning("Node %s reversion failed. Skipping this node for now.", node)

    # Generates a small random suffix used in load test deployment names.
    def _rand_suffix(self, n=5):
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

    # Creates a "load generator" Deployment in a namespace annotated to a YuniKorn queue,
    # with pods that sleep and hold requested resources.
    def create_queue_load(
        self,
        namespace: str,
        queue_path: str,
        replicas: int = 100,
        cpu_request: str = "1000m",
        mem_request: str = "2Gi",
        image: str = "busybox",
        name_prefix: str = "load-test",
    ) -> str:
        apps = AppsV1Api()
        name = f"{name_prefix}-{self._rand_suffix()}"

        labels = {"app": name_prefix, "component": "load-test"}
        annotations = {self.yk_queue_annotation: queue_path}

        container = V1Container(
            name="sleep",
            image=image,
            command=["sh", "-c", "sleep 9999999"],
            resources=V1ResourceRequirements(
                requests={"cpu": cpu_request, "memory": mem_request},
                limits={"cpu": cpu_request, "memory": mem_request},
            ),
        )

        pod_template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(labels=labels, annotations=annotations),
            spec=V1PodSpec(containers=[container], restart_policy="Always"),
        )

        dep = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name=name, labels=labels),
            spec=V1DeploymentSpec(
                replicas=replicas,
                selector=V1LabelSelector(match_labels=labels),
                template=pod_template,
            ),
        )

        apps.create_namespaced_deployment(namespace=namespace, body=dep)
        logger.info(
            f"Created loadgen deployment {name} in ns={namespace} queue={queue_path} "
            f"replicas={replicas} cpu={cpu_request} mem={mem_request}"
        )
        return name

    # Scales an existing load test Deployment to a new replica count.
    def scale_queue_load(self, namespace: str, deployment_name: str, replicas: int):
        apps = AppsV1Api()
        body = {"spec": {"replicas": replicas}}
        apps.patch_namespaced_deployment_scale(name=deployment_name, namespace=namespace, body=body)
        logger.info(f"Scaled loadgen deployment {deployment_name} to {replicas} replicas in ns={namespace}")

    # Deletes the load test Deployment from the namespace.
    def delete_queue_load(self, namespace: str, deployment_name: str):
        apps = AppsV1Api()
        apps.delete_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body={"propagationPolicy": "Foreground", "gracePeriodSeconds": 0},
        )
        logger.info(f"Deleted loadgen deployment {deployment_name} in ns={namespace}")

    # Determines how many nodes are needed for a namespace/queue based on:
    #   - total demand vs headroom-adjusted max capacity
    #   - pending demand vs available (max - used)
    #   - binpack deadlock heuristic (pending exists, perfectly filled pattern)
    # Returns 0 if no trigger; otherwise returns at least 1 node.
    def _needed_nodes_for_ns(self, ns, queue_path, headroom):
        try:
            max_vcores, max_mem = self.get_queue_max_caps(queue_path)
        except Exception as e:
            logger.error(f"Failed to read queue caps for {queue_path}: {e}")
            return 0

        used_v, used_m, pend_v, pend_m, pend_count, max_pend_cpu, max_pend_mem = self.get_queue_usage_and_pending(ns, queue_path)

        cap_v = max_vcores * headroom
        cap_m = max_mem * headroom

        total_demand_v = used_v + pend_v
        total_demand_m = used_m + pend_m

        avail_v = max(0.0, max_vcores - used_v)
        avail_m = max(0.0, max_mem - used_m)

        over_total_v = total_demand_v > cap_v
        over_total_m = total_demand_m > cap_m
        over_pending_v = pend_v > avail_v
        over_pending_m = pend_m > avail_m

        pods_per_node_cpu = int(self.node_vcores // max_pend_cpu) if max_pend_cpu > 0 else 0
        pods_per_node_mem = int(self.node_mem_bytes // max_pend_mem) if max_pend_mem > 0 else 0
        pods_per_node = min(pods_per_node_cpu, pods_per_node_mem)
        running_pods_est = int(round(used_v / max(max_pend_cpu, 1))) if max_pend_cpu > 0 else 0

        binpack_blocked = (
            pend_count > 0 and
            pods_per_node > 0 and
            running_pods_est > 0 and
            (running_pods_est % pods_per_node == 0)
        )

        if binpack_blocked:
            logger.warning(
                "[%s|%s] BINPACK BLOCKED: used_v=%.2f max_pend_cpu=%.2f pods_per_node=%d running_pods_est=%d pend_count=%d",
                ns, queue_path, used_v, max_pend_cpu, pods_per_node, running_pods_est, pend_count
            )

        need_v = over_total_v or over_pending_v or binpack_blocked
        need_m = over_total_m or over_pending_m or binpack_blocked

        if not (need_v or need_m):
            if pend_count > 0 and pend_v == 0.0 and pend_m == 0.0:
                logger.info(
                    f"[{ns}|{queue_path}] Pending pods detected (count={pend_count}) but no resource demand; "
                    f"check pod requests/limits or defaults (cpu={self.default_cpu_request_m}m, mem={self.default_mem_request})."
                )
            logger.info(
                f"[{ns}|{queue_path}] no trigger: used={used_v:.2f}/{max_vcores}, pend={pend_v:.2f} (pods={pend_count}); "
                f"used_mem={int(used_m)}/{int(max_mem)}, pend_mem={int(pend_m)}"
            )
            return 0

        deficit_v = max(0.0, total_demand_v - cap_v)
        deficit_m = max(0.0, total_demand_m - cap_m)

        need_nodes_v = math.ceil(deficit_v / float(self.node_vcores)) if deficit_v > 0 else 0
        need_nodes_m = math.ceil(deficit_m / float(self.node_mem_bytes)) if deficit_m > 0 else 0

        need_nodes = max(need_nodes_v, need_nodes_m, 1)

        logger.info(
            f"[{ns}|{queue_path}] TRIGGER: total_demand_v={total_demand_v:.2f} cap_v={cap_v:.2f} "
            f"def_v={deficit_v:.2f} -> nodes_v={need_nodes_v}; "
            f"total_demand_m={int(total_demand_m)} cap_m={int(cap_m)} def_m={int(deficit_m)} -> nodes_m={need_nodes_m}; "
            f"need_nodes={need_nodes} (pend_pods={pend_count})"
        )
        return need_nodes

    # Evaluate pass:
    #   - computes remaining conversion headroom vs max_converted_nodes
    #   - for each monitored namespace, computes needed nodes
    #   - enqueues (ns, queue_path) tasks into the global queue (PENDING_CONVERSIONS)
    def trigger(self):
        """
        Evaluate all namespaces and enqueue conversion tasks into PENDING_CONVERSIONS.
        Honors the live cluster-wide cap every time it runs.
        """
        HEADROOM = 0.95

        total_converted, per_ns_counts = self.get_converted_counts()
        remaining_overall = max(0, self.max_converted_nodes - total_converted)

        multi_info = per_ns_counts.pop("multiple", None)
        ns_only_counts = per_ns_counts

        if multi_info:
            if isinstance(multi_info, dict):
                multi_count = multi_info.get("count", 0)
                multi_details = multi_info.get("details", [])
                logger.info(
                    f"Converted nodes (live): total={total_converted}, per_ns={ns_only_counts}, "
                    f"multiple={multi_count} (namespace overlaps={multi_details}), "
                    f"max={self.max_converted_nodes}, remaining={remaining_overall}"
                )
            else:
                logger.info(
                    f"Converted nodes (live): total={total_converted}, per_ns={ns_only_counts}, "
                    f"multiple={multi_info}, "
                    f"max={self.max_converted_nodes}, remaining={remaining_overall}"
                )
        else:
            logger.info(
                f"Converted nodes (live): total={total_converted}, per_ns={ns_only_counts}, "
                f"max={self.max_converted_nodes}, remaining={remaining_overall}"
            )

        if remaining_overall <= 0:
            return 0

        to_enqueue = []
        still_available = remaining_overall
        for ns in self.monitored_namespaces:
            queue_path = self.namespace_queue_paths.get(ns)
            if not queue_path:
                logger.warning(f"No YuniKorn queue path configured for namespace '{ns}'")
                continue

            need = self._needed_nodes_for_ns(ns, queue_path, HEADROOM)
            if need <= 0:
                continue

            take = min(need, still_available)
            if take <= 0:
                break

            to_enqueue.extend([(ns, queue_path)] * take)
            still_available -= take

            logger.info(f"[{ns}] enqueue {take} conversions (requested={need}, remaining_overall now {still_available})")

            if still_available <= 0:
                break

        if to_enqueue:
            with PENDING_LOCK:
                PENDING_CONVERSIONS.extend(to_enqueue)
            logger.info(f"Queued {len(to_enqueue)} conversion task(s). Queue depth is now {len(PENDING_CONVERSIONS)}.")

        return len(to_enqueue)

    # Runs forever on a timer:
    #   - calls trigger() to enqueue conversions
    #   - calls check_converted_nodes() to revert idle nodes
    def evaluate_loop(self):
        logger.info("Evaluate loop started")
        while True:
            try:
                self.trigger()
                self.check_converted_nodes()
            except Exception as e:
                logger.error(f"Evaluate loop error: {e}")
            time.sleep(self.check_interval_seconds)

    # Runs forever:
    #   - enforces global conversion cap
    #   - drains one task from PENDING_CONVERSIONS at a time
    #   - executes conversion (node-convert + discovery + capacity bump)
    def actuator_loop(self):
        logger.info("Actuator loop started")
        while True:
            try:
                total_converted, _ = self.get_converted_counts()
                remaining_overall = max(0, self.max_converted_nodes - total_converted)
                task = None

                if remaining_overall <= 0:
                    time.sleep(2)
                    continue

                now = time.time()
                if now < self._next_conversion_allowed_at:
                    wait_time = self._next_conversion_allowed_at - now
                    logger.info(f"Actuator: global cooldown active ({wait_time:.1f}s remaining). Waiting...")
                    time.sleep(1)
                    continue

                with PENDING_LOCK:
                    if PENDING_CONVERSIONS:
                        task = PENDING_CONVERSIONS.pop(0)

                if not task:
                    time.sleep(1)
                    continue

                ns, queue_path = task
                logger.info(f"Actuator: converting one node for ns={ns}, queue={queue_path} (remaining cap={remaining_overall})")
                node = self.convert_node_to_k8s(ns, queue_path)
                if node:
                    logger.info(f"Actuator: converted node {node} for ns={ns}")
                else:
                    logger.warning(f"Actuator: conversion failed for ns={ns}; will not requeue automatically")
            except Exception as e:
                logger.error(f"Actuator loop error: {e}")
                time.sleep(2)

    # Entry point for monitor mode:
    #   - logs basic cluster info
    #   - starts evaluate + actuator threads
    #   - keeps the main thread alive
    def monitor(self):
        try:
            nodes = self.v1.list_node().items
            logger.info(f"There are {len(nodes)} nodes in the cluster.")
        except ApiException as e:
            logger.error(f"Failed to retrieve nodes: {e}")

        logger.info("Starting service loops (evaluate + actuator)")
        t1 = Thread(target=self.evaluate_loop, name="dnm-evaluate", daemon=True)
        t2 = Thread(target=self.actuator_loop, name="dnm-actuator", daemon=True)
        t1.start()
        t2.start()

        while True:
            time.sleep(60)


# CLI entrypoint:
#   - parses args
#   - constructs the manager (loads config + kubeconfig)
#   - runs either monitor mode or test mode load generation
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamic Node Manager")
    parser.add_argument(
        "--mode",
        choices=["monitor", "test"],
        default="monitor",
        help="Run in monitor mode (default) or test load generation mode.",
    )
    parser.add_argument("--namespace", help="Namespace for test load")
    parser.add_argument("--queue", help="Queue path for test load")
    parser.add_argument("--replicas", type=int, default=50, help="Replicas for test load")
    args = parser.parse_args()

    mgr = DynamicNodeManager()

    if args.mode == "monitor":
        mgr.monitor()
    elif args.mode == "test":
        if not args.namespace or not args.queue:
            parser.error("--namespace and --queue are required for test mode")
        dep_name = mgr.create_queue_load(
            namespace=args.namespace,
            queue_path=args.queue,
            replicas=args.replicas,
            cpu_request="1000m",
            mem_request="2Gi",
        )
        print(f"Test deployment created: {dep_name}")
