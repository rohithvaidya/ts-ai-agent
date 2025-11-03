# Prometheus-Powered MCP AI Observability Agent

This project implements a **Model Context Protocol (MCP)** that provides intelligent, real-time observability over Kubernetes clusters using **Prometheus metrics** and **LLM-based reasoning**.  

It exposes monitoring tools as callable APIs that an **AI assistant** or **chatbot** can query using natural language.


## ⚙️ Prerequisites

You’ll need the following installed:

- **Python 3.9+**
- **Minikube** (for running Kubernetes clusters)
- **Prometheus** (deployed on each cluster)
- **Ollama** (for local LLM inference)
- **FastMCP** Python package

---

## 🧰 Configuration

Edit `config/{}_config.yaml` as follows:

```yaml
# Server Config
mcp_server_url: "http://localhost:8001/mcp"


# LLM Configuration
ollama_url: "http://localhost:11434"
ollama_model: "qwen2.5-coder:7b"

# Prometheus Instances
prometheus_instances:
  - name: prometheus_1
    base_url: "http://localhost:9090"
    headers: {}
    disable_ssl: false
```

##  Setting Up Prometheus

You can setup prometheus using docker by following the below steps

```bash
cat <<EOF > write.yml
global:
  scrape_interval: 15s

remote_write:
  - url: "http://localhost:9090/api/v1/write"
    queue_config:
      max_samples_per_send: 100000
      capacity: 100000

storage:
  tsdb:
    out_of_order_time_window: 15d
EOF
```

Run on docker

```bash
docker run -d \
  --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/write.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.enable-remote-write-receiver
```

Push some sample data using utils/prometheus_data_pusher
```bash
{
  "prometheus_url": "http://localhost:9090/api/v1/write",
  "auth_token": null,
  "num_clusters": 2,
  "nodes_per_cluster": 2,
  "namespaces_per_cluster": 2,
  "pods_per_namespace": 2,
  "containers_per_pod": 1,
  "scrape_interval": 120,
  "batch_size": 20,
  "days_of_history": 1
}
```

You can also simulate a multi-cluster environment using two Minikube clusters (Optional):

```yaml
# Create two clusters
minikube start -p minikube1
minikube start -p minikube2

Enable Prometheus in both clusters:

kubectl create namespace monitoring
kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/bundle.yaml

Forward ports locally:

# Cluster 1
kubectl --context=minikube1 -n monitoring port-forward svc/prometheus-operated 9090:9090

# Cluster 2
kubectl --context=minikube2 -n monitoring port-forward svc/prometheus-operated 9091:9090
```

Prometheus instances are now accessible at:

    http://localhost:9090 (Cluster 1)

    http://localhost:9091 (Cluster 2)

## 🚀 Running the MCP Server

Start the MCP server: 

```bash
fastmcp run server.py:app --transport http --port 8001
```

Run the client

```bash
python3 client_static.py
```

## 🧪 Sample Tool Calls 


```bash
1) Get top 5 pods by cpu usage
2) What is the health of the cluster
3) Get node disk usage info for last 10 minutes
```
