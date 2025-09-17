import requests


base_url = "http://localhost:8080/api/v1"
auth = ("admin", "admin")
headers = {"Content-Type": "application/json"}

# API 호출
dags_url = f"{base_url}/dags"
response = requests.get(dags_url, auth=auth)
dags = response.json()
print("DAG 목록:")
for dag in dags.get("dags", []):
    print(f"- {dag['dag_id']}")

dag_id = input("실행할 DAG ID를 입력하세요: ")

# DAG trigger
trigger_url = f"{base_url}/dags/{dag_id}/dagRuns"
data = {"conf": {}}
trigger_response = requests.post(trigger_url, json=data, headers=headers, auth=auth)
print("Trigger 응답:")
print(trigger_response.json())
