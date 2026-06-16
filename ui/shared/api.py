import requests

BASE_URL = "http://localhost:8000"
API_KEY = "test-key"


def get_latest_result(dag_name: str, meter_id: str):
    url = f"{BASE_URL}/v1/data-apps/{dag_name}/meters/{meter_id}/results/latest"

    response = requests.get(
        url,
        headers={"X-API-Key": API_KEY},
        timeout=30,
    )

    response.raise_for_status()

    return response.json()