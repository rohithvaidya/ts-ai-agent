# mcp_server.py
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import yaml
import os

from datetime import datetime, timedelta
from typing import Dict, Any

from fastmcp import FastMCP
from prometheus_api_client import PrometheusConnect
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi

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
    window: str = "30m"
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
def node_disk_usage(window_minutes: int = 20) -> Dict[str, Any]:
    """
    Summarized node disk usage (%) for important mount points across Prometheus clients.

    Args:
        window_minutes (int): Lookback window in minutes (default: 20).

    Returns:
        Dict[str, Any]: Aggregated disk usage per node.
    """

    if not prometheus_clients:
        return {"error": "No Prometheus clients initialized"}

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=window_minutes)
    step = "1m"  # 1-minute resolution

    important_mounts = {"/", "/var/lib", "/data"}
    all_results = {}

    for prom_name, client in prometheus_clients.items():
        try:
            query = """
            100 * (1 - (node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} 
                        / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"}))
            """

            result = client.custom_query_range(
                query=query.strip(),
                start_time=start_time,
                end_time=end_time,
                step=step
            )

            disk_usage = []
            for item in result:
                metric = item.get("metric", {})
                mount = metric.get("mountpoint", "")
                if mount not in important_mounts:
                    continue

                node = metric.get("node", "unknown")
                cluster = metric.get("cluster", "unknown")
                region = metric.get("region", "unknown")
                environment = metric.get("environment", "unknown")

                # Average usage across time range
                values = [float(v[1]) for v in item.get("values", [])]
                if not values:
                    continue
                avg_usage = sum(values) / len(values)

                disk_usage.append({
                    "node": node,
                    "mount": mount,
                    "cluster": cluster,
                    "region": region,
                    "environment": environment,
                    "avg_disk_usage_percent": round(avg_usage, 2),
                    "max_disk_usage_percent": round(max(values), 2),
                })

            disk_usage.sort(key=lambda x: x["max_disk_usage_percent"], reverse=True)

            all_results[prom_name] = {
                "query": query.strip(),
                "window_minutes": window_minutes,
                "timestamp": end_time.isoformat(),
                "top_nodes": disk_usage[:10],
            }

        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {
        "node_disk_usage_per_prometheus": all_results,
        "fetched_at": datetime.utcnow().isoformat(),
    }



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
                status_msg = f"{failed} pods are failing. {running}/{total} pods are running."
            elif pending > 0:
                status_msg = f"{pending} pods are pending. {running}/{total} are running fine."
            else:
                status_msg = f"All systems nominal: {running}/{total} pods are healthy."

            all_results[prom_name] = {"summary": summary, "message": status_msg}
        except Exception as e:
            all_results[prom_name] = {"error": str(e)}

    return {"cluster_health_per_prometheus": all_results, "timestamp": datetime.now().isoformat()}



if __name__ == "__main__":
    app.run()