import asyncio
import json
import httpx
import re
import os
import yaml
from fastmcp import Client


def load_config(path="config.yaml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f)


# Load configs
ollama_config = load_config("../../config/ollama_config.yaml")
server_config = load_config("../../config/mcp_server_config.yaml")

OLLAMA_API_URL = ollama_config.get("ollama_url")
MODEL_NAME = ollama_config.get("ollama_model")
client = Client(server_config.get("mcp_server_url", "http://localhost:8001/mcp"))


async def ask_ollama_stream(prompt: str):
    """Stream text completions from Ollama"""
    async with httpx.AsyncClient(timeout=None) as session:
        async with session.stream(
            "POST",
            f"{OLLAMA_API_URL}/v1/completions",
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "max_tokens": 1000,
                "temperature": 0.0,
                "stream": True,
            },
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    chunk = line[6:]
                    if chunk != "[DONE]":
                        try:
                            data = json.loads(chunk)
                            text = data.get("choices", [{}])[0].get("text", "")
                            if text:
                                yield text
                        except json.JSONDecodeError:
                            continue


async def ask_ollama(prompt: str) -> str:
    """Simple non-streaming Ollama call"""
    async with httpx.AsyncClient(timeout=300.0) as session:
        resp = await session.post(
            f"{OLLAMA_API_URL}/v1/completions",
            json={
                "model": MODEL_NAME,
                "prompt": prompt,
                "max_tokens": 1000,
                "temperature": 0.0,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["text"]


async def infer_tool_call(nl_query: str) -> dict:
    """Ask LLM to produce a single tool call in JSON"""
    prompt = (
        "You are an assistant that converts a natural language query "
        "into a single MCP tool call in JSON format with 'tool_name', 'params' (dictionary)\n"
        "Return ONLY JSON (no explanations, no markdown).\n\n"
        "Available Tools:\n"
        "- current_metric_for_pods(metric_name: str = 'container_cpu_usage_seconds_total', pod_names: Optional[List[str]] = None)\n"
        "- top_n_pods_by_metric(metric_name: str = 'container_cpu_usage_seconds_total', top_n: int = 5, window: str = '30m')\n"
        "- node_disk_usage(window_minutes: int = 20)\n"
        "- describe_cluster_health()\n\n"
        f"Natural language query: {nl_query}"
    )

    llm_response = await ask_ollama(prompt)
    llm_response = re.sub(r"```(?:json)?", "", llm_response.strip())

    print(llm_response)

    try:
        tool_call = json.loads(llm_response)
        if not isinstance(tool_call, dict):
            tool_call = {"tool_name": str(tool_call), "params": {}}
    except json.JSONDecodeError:
        tool_call = {"tool_name": nl_query.strip(), "params": {}}

    return tool_call


async def run_query(nl_query: str):
    print(f"\n🔹 Query: {nl_query}")

    tool_call = await infer_tool_call(nl_query)
    tool_name = tool_call.get("tool_name")
    params = tool_call.get("params", {})

    print(f"🔧 Tool Call → {tool_name}({params})")

    async with client:
        try:
            result = await client.call_tool(tool_name, params)
        except Exception as e:
            result = {"error": str(e)}

    # Summarize result using streaming
    summary_prompt = f"relate the tool call result: {result} to the user input {nl_query} and summarise"
    print("\n🧩 Summary:\n")
    async for chunk in ask_ollama_stream(summary_prompt):
        print(chunk, end="", flush=True)
    print("\n")


if __name__ == "__main__":
    while True:
        query = input("\nEnter your query (or 'exit' to quit): ")
        if query.lower() == "exit":
            break
        asyncio.run(run_query(query))
