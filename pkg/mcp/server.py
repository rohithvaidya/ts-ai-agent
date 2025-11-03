# mcp_server.py
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import yaml
import os

from fastmcp import FastMCP
from prometheus_api_client import PrometheusConnect
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi

import sqlite3
import uuid
import numpy as np
import json
from dateutil import parser as dateparser
from datetime import timezone


app = FastMCP("Monitoring MCP Server")

prometheus_clients: Dict[str, PrometheusConnect] = {}

def load_config():
    
    config_dir = "../../config/"
    
    # Load Prometheus config
    prom_config_path = os.path.join(config_dir, "prometheus_config.yaml")
    prom_config = {}
    if os.path.exists(prom_config_path):
        with open(prom_config_path, 'r') as f:
            prom_config = yaml.safe_load(f)
    
    return prom_config

def initialize_clients():
    global prometheus_clients
    
    prom_config = load_config()
    
    for cfg in prom_config.get("prometheus_instances", []):
        name = cfg.get("name")
        try:
            prometheus_clients[name] = PrometheusConnect(
                url=cfg['base_url'],
                headers=cfg.get('headers', {}),
                disable_ssl=cfg.get('disable_ssl', False)
            )
            print(f"Initialized Prometheus client: {name} -> {cfg['base_url']}")
        except Exception as e:
            print(f"Failed to initialize Prometheus client {name}: {e}")

initialize_clients()


@app.tool()
def current_metric_for_pods(
    metric_name: str = "container_cpu_usage_seconds_total",
    pod_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get the current CPU usage for a list of pods directly using Prometheus query.
    """
    if not prometheus_clients:
        return {"error": "Prometheus client not initialized"}
    
    if not pod_names:
        return {"error": "No pods provided"}
    
    results = []
    all_results = {}
    for prom_name, client in prometheus_clients.items():
        results = []
        try:
            for pod_name in pod_names:
                # PromQL query for the given pod
                query = f"{metric_name}{{pod='{pod_name}'}}"
                
                # Query Prometheus for current value
                response = client.custom_query(query=query)
                
                # Extract latest value if available
                value = None
                if response and len(response) > 0:
                    try:
                        value = float(response[0]['value'][1])
                    except (KeyError, ValueError, IndexError):
                        value = None
                
                results.append({
                    "pod": pod_name,
                    "query": query,
                    "current_cpu_value": value
                })  
            
        except Exception as e:
            return {"error": f"Failed to query Prometheus: {str(e)}"}

        all_results[prom_name] = results

    return {
                "metric": metric_name,
                "pods_current_cpu_per_prometheus": all_results,
                "timestamp": datetime.now().isoformat()
            }

    
@app.tool()
def top_n_pods_by_metric(
    metric_name: str = "container_cpu_usage_seconds_total", 
    top_n: int = 5, 
    window: str = "5m"
) -> Dict[str, Any]:
    
    if not prometheus_clients:
        return {"error": "Prometheus client not initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        results = []
        try:
            # Filter metrics with a pod label
            query = f'topk({top_n}, avg_over_time({metric_name}{{pod!=""}}[{window}]))'
            result = client.custom_query(query=query)

            # Extract pod names and CPU usage values
            pods_info = []
            for item in result:
                metric = item.get("metric", {})
                pod_name = metric.get("pod")  # only include if pod exists
                value = float(item.get("value", [0, "0"])[1])
                if pod_name:
                    pods_info.append({"pod": pod_name, "value": value})

            # Sort by CPU usage descending
            pods_info.sort(key=lambda x: x["value"], reverse=True)

            

        except Exception as e:
            return {"error": str(e)}
        
        all_results[prom_name] = pods_info

    return {
            "pods_per_prometheus": all_results,
            "timestamp": datetime.now().isoformat()
        }

@app.tool()
def pod_network_io(pod_names: Optional[List[str]] = None) -> Dict[str, Any]:
    
    if not prometheus_clients:
        return {"error": "Prometheus client not initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        results = []
        try:
            results = []
            for pod_name in pod_names or []:
                rx_query = f'rate(container_network_receive_bytes_total{{pod="{pod_name}"}}[5m])'
                tx_query = f'rate(container_network_transmit_bytes_total{{pod="{pod_name}"}}[5m])'
                rx_result = client.custom_query(rx_query)
                tx_result = client.custom_query(tx_query)
                rx = float(rx_result[0]['value'][1]) if rx_result else 0
                tx = float(tx_result[0]['value'][1]) if tx_result else 0
                results.append({"pod": pod_name, "rx_bytes_per_sec": rx, "tx_bytes_per_sec": tx})
            
        except Exception as e:
            return {"error": str(e)}
        all_results[prom_name] = results
    
    return {"pod_network_io_per_promotheus": all_results, "timestamp": datetime.now().isoformat()}

@app.tool()
def pods_exceeding_cpu(threshold: float = 0.8) -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = f'rate(container_cpu_usage_seconds_total[5m]) > {threshold}'
            result = client.custom_query(query=query)
            pods = [{"pod": item["metric"]["pod"], "cpu_value": float(item["value"][1])} 
                    for item in result if "pod" in item["metric"]]
            all_results[prom_name] = pods
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {
        "pods_exceeding_cpu_per_prometheus": all_results,
        "threshold": threshold,
        "timestamp": datetime.now().isoformat()
    }


@app.tool()
def pod_status_summary() -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = 'sum(kube_pod_status_phase) by (phase)'
            result = client.custom_query(query=query)
            status_summary = {item["metric"]["phase"]: int(float(item["value"][1])) for item in result}
            total = sum(status_summary.values())
            status_summary["total"] = total
            all_results[prom_name] = status_summary
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {
        "pod_status_summary_per_prometheus": all_results,
        "timestamp": datetime.now().isoformat()
    }

@app.tool()
def recent_pod_events(limit: int = 10) -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = 'sort_desc(sum by (reason, involved_object_name) (increase(kube_event_count[10m])))'
            result = client.custom_query(query=query)
            
            events = []
            for item in result[:limit]:
                metric = item.get("metric", {})
                events.append({
                    "pod": metric.get("involved_object_name"),
                    "reason": metric.get("reason"),
                    "count": int(float(item["value"][1]))
                })
            all_results[prom_name] = events
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {
        "recent_pod_events_per_prometheus": all_results,
        "lookback": "10m",
        "timestamp": datetime.now().isoformat()
    }


@app.tool()
def node_disk_usage() -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = """
            100 * (1 - (node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} 
                        / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"}))
            """
            result = client.custom_query(query=query)
            disk_usage = []
            for item in result:
                metric = item.get("metric", {})
                node = metric.get("instance")
                mount = metric.get("mountpoint", "")
                usage = float(item.get("value", [0, "0"])[1])
                disk_usage.append({"node": node, "mount": mount, "disk_usage_percent": round(usage, 2)})
            all_results[prom_name] = disk_usage
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"node_disk_usage_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}


@app.tool()
def describe_cluster_health() -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = 'sum(kube_pod_status_phase) by (phase)'
            result = client.custom_query(query=query)
            summary = {item["metric"]["phase"]: int(float(item["value"][1])) for item in result}
            total = sum(summary.values())
            running = summary.get("Running", 0)
            pending = summary.get("Pending", 0)
            failed = summary.get("Failed", 0)

            if failed > 0:
                status_msg = f"⚠️ {failed} pods are failing. {running}/{total} pods are running."
            elif pending > 0:
                status_msg = f"⏳ {pending} pods are pending. {running}/{total} are running fine."
            else:
                status_msg = f"✅ All systems nominal: {running}/{total} pods are healthy."

            all_results[prom_name] = {"summary": summary, "message": status_msg}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"cluster_health_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}


@app.tool()
def top_disk_pressure_nodes(threshold: float = 80.0, top_n: int = 5) -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = """
            100 * (1 - (node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} 
                        / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"}))
            """
            result = client.custom_query(query=query)
            nodes_info = []
            for item in result:
                metric = item.get("metric", {})
                node = metric.get("instance")
                mount = metric.get("mountpoint", "")
                usage = float(item.get("value", [0, "0"])[1])
                if usage >= threshold:
                    nodes_info.append({"node": node, "mount": mount, "usage_percent": round(usage, 2)})

            nodes_info.sort(key=lambda x: x["usage_percent"], reverse=True)
            nodes_info = nodes_info[:top_n]

            msg = f"⚠️ {len(nodes_info)} nodes above {threshold}% disk usage." if nodes_info else "✅ No nodes are under disk pressure."
            all_results[prom_name] = {"nodes": nodes_info, "message": msg, "threshold": threshold}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"top_disk_pressure_nodes_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



@app.tool()
def pod_restart_trend(window: str = "30m", top_n: int = 5) -> Dict[str, Any]:
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = f'topk({top_n}, increase(kube_pod_container_status_restarts_total[{window}]))'
            result = client.custom_query(query=query)
            restart_trends = []
            for item in result:
                metric = item.get("metric", {})
                pod = metric.get("pod")
                container = metric.get("container", "")
                restarts = float(item.get("value", [0, "0"])[1])
                if pod:
                    restart_trends.append({"pod": pod, "container": container, "restarts": restarts})

            restart_trends.sort(key=lambda x: x["restarts"], reverse=True)
            msg = f"⚠️ Pods with recent restarts detected (last {window})." if restart_trends else f"✅ No recent restarts in the last {window}."
            all_results[prom_name] = {"pods": restart_trends, "message": msg, "window": window}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"pod_restart_trend_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}


@app.tool()
def detect_pod_anomalies(metric_name="container_cpu_usage_seconds_total", z_threshold=3.0):
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = f'avg_over_time({metric_name}{{pod!=""}}[15m])'
            result = client.custom_query(query=query)
            values = [float(r["value"][1]) for r in result]
            if not values:
                all_results[prom_name] = {"message": "No data"}
                continue

            mean = sum(values)/len(values)
            std = (sum((x-mean)**2 for x in values)/len(values))**0.5
            anomalies = []
            for r in result:
                pod = r["metric"].get("pod")
                val = float(r["value"][1])
                z = (val - mean)/std if std > 0 else 0
                if abs(z) > z_threshold:
                    anomalies.append({"pod": pod, "value": val, "z_score": round(z,2)})

            all_results[prom_name] = {"anomalies": anomalies, "mean": mean, "std": std}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"pod_anomalies_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}


@app.tool()
def namespace_resource_summary(resource="cpu", window="5m"):
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    metric = "container_cpu_usage_seconds_total" if resource=="cpu" else "container_memory_usage_bytes"

    for prom_name, client in prometheus_clients.items():
        try:
            query = f'sum(rate({metric}{{namespace!=""}}[{window}])) by (namespace)'
            result = client.custom_query(query=query)
            usage = [{"namespace": r["metric"]["namespace"], "value": float(r["value"][1])} for r in result]
            total = sum(x["value"] for x in usage)
            for x in usage:
                x["percent_of_total"] = round((x["value"]/total)*100, 2) if total > 0 else 0
            usage.sort(key=lambda x: x["value"], reverse=True)
            all_results[prom_name] = {"resource": resource, "usage_by_namespace": usage}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"namespace_resource_summary_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



@app.tool()
def detect_crashloop_pods(window="10m", threshold=2):
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = f'increase(kube_pod_container_status_restarts_total[{window}]) > {threshold}'
            result = client.custom_query(query=query)
            pods = [{"pod": r["metric"]["pod"], "restarts": int(float(r["value"][1]))} for r in result if "pod" in r["metric"]]
            all_results[prom_name] = {"crashloop_pods": pods, "window": window}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"crashloop_pods_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}


@app.tool()
def correlate_metrics(metric_a="container_cpu_usage_seconds_total", metric_b="container_network_receive_bytes_total", window="10m"):
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    import numpy as np
    all_results = {}

    for prom_name, client in prometheus_clients.items():
        try:
            r1 = client.custom_query(f'rate({metric_a}[{window}])')
            r2 = client.custom_query(f'rate({metric_b}[{window}])')
            data_a = {r["metric"].get("pod"): float(r["value"][1]) for r in r1 if "pod" in r["metric"]}
            data_b = {r["metric"].get("pod"): float(r["value"][1]) for r in r2 if "pod" in r["metric"]}
            common_pods = set(data_a) & set(data_b)
            if not common_pods:
                all_results[prom_name] = {"message": "No overlapping pods"}
                continue
            pairs = [(data_a[p], data_b[p]) for p in common_pods]
            corr = float(np.corrcoef([x for x, _ in pairs], [y for _, y in pairs])[0,1])
            all_results[prom_name] = {"correlation": round(corr, 3), "metric_a": metric_a, "metric_b": metric_b, "window": window}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"correlation_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



@app.tool()
def pod_event_timeline(pod_name: str, window: str = "30m"):
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    queries = {
        "restarts": f'increase(kube_pod_container_status_restarts_total{{pod="{pod_name}"}}[{window}])',
        "network_rx": f'rate(container_network_receive_bytes_total{{pod="{pod_name}"}}[{window}])',
        "cpu": f'rate(container_cpu_usage_seconds_total{{pod="{pod_name}"}}[{window}])',
    }

    for prom_name, client in prometheus_clients.items():
        try:
            timeline = {}
            for key, q in queries.items():
                result = client.custom_query(q)
                if result:
                    timeline[key] = float(result[0]["value"][1])
            all_results[prom_name] = {"pod": pod_name, "timeline": timeline, "window": window}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"pod_event_timeline_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



@app.tool()
def node_condition_summary():
    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    all_results = {}
    for prom_name, client in prometheus_clients.items():
        try:
            query = 'kube_node_status_condition{status="true", condition!="Ready"}'
            result = client.custom_query(query=query)
            issues = [{"node": r["metric"]["node"], "condition": r["metric"]["condition"]} for r in result]
            all_results[prom_name] = {"node_issues": issues}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"node_condition_summary_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



def _ensure_sqlite_db(db_path="semantic_blobs.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS semantic_blobs (
        id TEXT PRIMARY KEY,
        cluster TEXT,
        level TEXT,
        metric TEXT,
        window TEXT,
        blob_json TEXT,
        created_at TEXT
    )
    """)
    conn.commit()
    return conn

def _iso_now_utc():
    return datetime.now(timezone.utc).isoformat()

def _parse_time_or_default(ts: Optional[str], default: datetime):
    if ts is None:
        return default
    try:
        return dateparser.isoparse(ts)
    except Exception:
        return default

# mapping which label identifies the entity at each level
LEVEL_LABEL_MAP = {
    "cluster": "cluster",
    "namespace": "namespace",
    "pod": "pod",
    "container": "container",
    "app": "app"
}

# default metrics per level (used if config missing)
DEFAULT_SUMMARY_METRICS = {
    "cluster": ["container_cpu_usage_seconds_total", "container_memory_usage_bytes"],
    "namespace": ["container_cpu_usage_seconds_total", "container_memory_usage_bytes"],
    "pod": ["container_cpu_usage_seconds_total", "container_memory_usage_bytes", "kube_pod_container_status_restarts_total"],
    "container": ["container_cpu_usage_seconds_total", "container_memory_usage_bytes", "container_network_transmit_bytes_total", "container_network_receive_bytes_total"]
}

@app.tool()
def summarise_tsdb(
    from_ts: Optional[str] = None,
    to_ts: Optional[str] = None,
    step: str = "60s",
    db_path: str = "semantic_blobs.db",
    z_anomaly_threshold: float = 3.0
) -> Dict[str, Any]:
    """
    Summarize Prometheus time series into LLM-friendly JSON blobs and store them in sqlite.
    - from_ts, to_ts: ISO strings. Default: to_ts = now UTC, from_ts = now - 3 hours.
    - step: Prometheus query_range step (default 60s).
    - Reads 'summary_levels' mapping from prometheus_config.yaml (fallback to defaults).
    """

    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    # Parse times
    now = datetime.now(timezone.utc)
    default_to = now
    default_from = now - timedelta(hours=3)

    end_dt = _parse_time_or_default(to_ts, default_to)
    start_dt = _parse_time_or_default(from_ts, default_from)

    # Read config for summary levels & metrics
    prom_cfg = load_config()
    summary_levels = prom_cfg.get("summary_levels", DEFAULT_SUMMARY_METRICS)

    conn = _ensure_sqlite_db(db_path)
    cur = conn.cursor()

    stored_blob_ids = []
    # temporary in-memory mapping to help link children -> parents
    blobs_by_level_and_entity = {}  # e.g. ("pod","api-123") -> blob_id

    for prom_name, client in prometheus_clients.items():
        # iterate configured levels
        for level, metrics in summary_levels.items():
            label_key = LEVEL_LABEL_MAP.get(level, level)
            for metric in metrics:
                try:
                    # query Prometheus for range series grouped by the label_key (we fetch all series and then group)
                    # Using query_range for richness; fallback to avg_over_time if query_range not available.
                    # We'll request step as provided.
                    query = metric
                    # Make a ranged query: the Prometheus API returns series; use client.custom_query_range
                    raw = []
                    try:
                        raw = client.custom_query_range(query=query, start_time=start_dt, end_time=end_dt, step=step)
                    except Exception:
                        # fallback to fetching instant avg_over_time per entity (less ideal)
                        window = f"{int((end_dt - start_dt).total_seconds())}s"
                        fallback_q = f'avg_over_time({metric}[{window}])'
                        raw = client.custom_query(fallback_q)

                    # raw is list of series objects. Each has "metric" dict and "values" (for range) or "value" (instant)
                    # Build per-entity timeseries
                    per_entity = {}
                    for series in raw:
                        metric_meta = series.get("metric", {})
                        if label_key not in metric_meta:
                            # Skip series that don't correspond to this level's label
                            continue
                        entity = metric_meta.get(label_key)
                        # extract timeseries datapoints
                        values = []
                        if "values" in series:
                            # range result: list of [ts, value] pairs
                            for ts_val in series["values"]:
                                try:
                                    values.append((float(ts_val[0]), float(ts_val[1])))
                                except Exception:
                                    continue
                        elif "value" in series:
                            # instant result - single point; expand to a single item
                            try:
                                ts = float(series["value"][0])
                                val = float(series["value"][1])
                                values.append((ts, val))
                            except Exception:
                                pass
                        if not values:
                            continue
                        per_entity.setdefault(entity, []).extend(values)

                    # For each entity, compute stats and create blob
                    for entity, timeseries in per_entity.items():
                        # Sort by time, dedupe by time if duplicates
                        timeseries = sorted({t: v for t, v in timeseries}.items())
                        times = np.array([t for t, v in timeseries], dtype=float)
                        vals = np.array([v for t, v in timeseries], dtype=float)

                        if len(vals) == 0:
                            continue

                        mean_v = float(np.mean(vals))
                        std_v = float(np.std(vals))
                        # Trend: linear slope over normalized time
                        if len(times) >= 2 and np.ptp(times) > 0:
                            # normalize times to seconds relative to start to avoid numeric issues
                            times_norm = (times - times[0]) / max(1.0, np.ptp(times))
                            try:
                                slope, intercept = np.polyfit(times_norm, vals, 1)
                                trend = "increasing" if slope > 0 else ("decreasing" if slope < 0 else "stable")
                            except Exception:
                                trend = "stable"
                        else:
                            trend = "stable"

                        # anomaly detection via z-score
                        anomalies = []
                        if std_v > 0:
                            z_scores = (vals - mean_v) / std_v
                            anomalous_indices = np.where(np.abs(z_scores) > z_anomaly_threshold)[0]
                            for idx in anomalous_indices:
                                anomalies.append({
                                    "timestamp": datetime.fromtimestamp(float(times[idx]), tz=timezone.utc).isoformat(),
                                    "value": float(vals[idx]),
                                    "z_score": float(round(z_scores[idx], 3))
                                })

                        start_iso = start_dt.isoformat()
                        end_iso = end_dt.isoformat()
                        window_str = f"{start_iso}/{end_iso}"

                        # blob id: deterministic-ish
                        blob_id = str(uuid.uuid4())
                        blob = {
                            "id": blob_id,
                            "level": level,
                            "cluster": prom_name,
                            "entity": entity,
                            "metric": metric,
                            "window": window_str,
                            "mean": mean_v,
                            "stddev": std_v,
                            "trend": trend,
                            "anomaly": anomalies,
                            "child_refs": [],    # populated after all blobs collected
                            "sample_count": int(len(vals)),
                            "created_at": _iso_now_utc()
                        }

                        # store mapping for linking
                        blobs_by_level_and_entity.setdefault(level, {})[ (prom_name, entity, metric, window_str) ] = blob_id

                        # save blob to sqlite
                        cur.execute(
                            "INSERT OR REPLACE INTO semantic_blobs (id, cluster, level, metric, window, blob_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (blob_id, prom_name, level, metric, window_str, json.dumps(blob), blob["created_at"])
                        )
                        conn.commit()
                        stored_blob_ids.append(blob_id)

                except Exception as exc:
                    # keep processing other metrics/levels even on error
                    print(f"[summarise_tsdb] error processing {prom_name}/{level}/{metric}: {exc}")

    # Build parent-child links (container -> pod -> namespace -> cluster)
    # We'll load all blobs and create child_refs by matching labels present inside blob entity relationships.
    cur.execute("SELECT id, blob_json FROM semantic_blobs")
    rows = cur.fetchall()
    id_to_blob = {}
    for _id, blob_json in rows:
        try:
            b = json.loads(blob_json)
            id_to_blob[_id] = b
        except Exception:
            continue

    # Helper: find blob ids for a given level and entity
    def find_blob_ids_for(level_name, cluster_name=None, entity_value=None, metric_name=None, window_str=None):
        res = []
        for _id, b in id_to_blob.items():
            if b.get("level") != level_name:
                continue
            if cluster_name and b.get("cluster") != cluster_name:
                continue
            if entity_value and b.get("entity") != entity_value:
                continue
            if metric_name and b.get("metric") != metric_name:
                continue
            if window_str and b.get("window") != window_str:
                continue
            res.append((_id, b))
        return res

    # Populate child_refs:
    # container -> pod
    # pod -> namespace
    # namespace -> cluster (not strictly necessary; cluster is top-level)
    for _id, blob in id_to_blob.items():
        lvl = blob.get("level")
        cluster = blob.get("cluster")
        ent = blob.get("entity")
        window_str = blob.get("window")
        if lvl == "container":
            # find pod-level blobs in same cluster and window where entity equals pod label
            # but we need pod label: we don't have labels for parent in blob; so we attempt a naming heuristic: if container entity contains pod name? 
            # Instead, try to find pod-level blobs with the same metric and same window that contain that container via scanning pod blobs for matching container in anomaly? 
            # Better approach: Use Prometheus label relationships - we attempt to query a mapping series to find parent pod for this container.
            # We'll try a lightweight mapping query to Prometheus: find most common pod for this container in the time window.
            try:
                mapping_q = f'topk(1, count by (pod) (container_cpu_usage_seconds_total{{container="{ent}"}}))'
                for prom_name, client in prometheus_clients.items():
                    if prom_name != cluster:
                        continue
                    try:
                        mapping_res = client.custom_query(mapping_q)
                        if mapping_res:
                            pod_name = mapping_res[0].get("metric", {}).get("pod")
                            if pod_name:
                                parents = find_blob_ids_for("pod", cluster_name=cluster, entity_value=pod_name, window_str=window_str)
                                if parents:
                                    # pick first parent
                                    parent_id = parents[0][0]
                                    # update child's blob
                                    blob["child_refs"] = blob.get("child_refs", [])
                                    # container should reference parent? The original design had child_refs pointing to children;
                                    # We'll treat child_refs as references to related blobs. For container, reference its pod parent.
                                    blob["child_refs"].append(parent_id)
                    except Exception:
                        pass
            except Exception:
                pass

        elif lvl == "pod":
            # find container blobs in same cluster and window whose labels indicate they belong to this pod
            # simple heuristic: search container-level blobs where we can find a mapping: count by (container) with pod label equals this pod.
            try:
                # query which containers have this pod label in the window
                # Use instant query to find containers with this pod label
                containers = []
                for prom_name, client in prometheus_clients.items():
                    if prom_name != cluster:
                        continue
                    q = f'count by (container) (container_cpu_usage_seconds_total{{pod="{ent}"}})'
                    try:
                        res = client.custom_query(q)
                        for r in res:
                            cont = r.get("metric", {}).get("container")
                            if cont:
                                containers.append(cont)
                    except Exception:
                        continue
                # match containers to container-level blobs and add child_refs
                for cont in containers:
                    children = find_blob_ids_for("container", cluster_name=cluster, entity_value=cont, window_str=window_str)
                    for child_id, _ in children:
                        blob.setdefault("child_refs", []).append(child_id)
            except Exception:
                pass

        elif lvl == "namespace":
            # find pods in this namespace
            try:
                pods = []
                for prom_name, client in prometheus_clients.items():
                    if prom_name != cluster:
                        continue
                    q = f'count by (pod) (container_cpu_usage_seconds_total{{namespace="{ent}"}})'
                    try:
                        res = client.custom_query(q)
                        for r in res:
                            p = r.get("metric", {}).get("pod")
                            if p:
                                pods.append(p)
                    except Exception:
                        continue
                for p in pods:
                    children = find_blob_ids_for("pod", cluster_name=cluster, entity_value=p, window_str=window_str)
                    for child_id, _ in children:
                        blob.setdefault("child_refs", []).append(child_id)
            except Exception:
                pass

        # update blob in DB if child_refs added
        try:
            cur.execute("UPDATE semantic_blobs SET blob_json = ? WHERE id = ?", (json.dumps(blob), _id))
            conn.commit()
        except Exception:
            pass

    conn.close()

    print("[summarise_tsdb] Stored blobs:", len(stored_blob_ids))

    return {
        "stored_blobs_count": len(stored_blob_ids),
        "db_path": db_path,
        "from": start_dt.isoformat(),
        "to": end_dt.isoformat(),
        "timestamp": _iso_now_utc()
    }


if __name__ == "__main__":
    app.run()
