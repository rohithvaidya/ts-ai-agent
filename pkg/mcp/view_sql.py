import sqlite3
import json

def fetch_all_semantic_blobs(db_path="semantic_blobs_test.db"):
    # Connect to the SQLite database file
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all rows
    cursor.execute("SELECT * FROM semantic_blobs")
    rows = cursor.fetchall()

    # Print in a formatted way
    if not rows:
        print("No records found in 'semantic_blobs' table.")
    else:
        print(f"Found {len(rows)} records:\n")
        for row in rows:
            id_, cluster, level, metric, window, blob_json, created_at = row
            print(f"ID: {id_}")
            print(f"Cluster: {cluster}")
            print(f"Level: {level}")
            print(f"Metric: {metric}")
            print(f"Window: {window}")
            print(f"Created At: {created_at}")
            try:
                blob = json.loads(blob_json)
                print("Blob JSON:", json.dumps(blob, indent=2))
            except json.JSONDecodeError:
                print("Blob JSON (raw):", blob_json)
            print("-" * 60)

    # Close connection
    conn.close()

if __name__ == "__main__":
    fetch_all_semantic_blobs("semantic_blobs_test.db")
