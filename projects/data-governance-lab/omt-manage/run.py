import argparse
import os
import requests
import json
import copy
from datetime import datetime, timezone
from payload import DATA

JWT_TOKEN = os.environ["JWT_TOKEN"]
HOST_URL = os.getenv("OMT_HOST", "http://localhost:8585")


def get_headers():
    return {"Content-Type": "application/json", "Authorization": f"Bearer {JWT_TOKEN}"}


def _create_service_entry(category: str, payload: dict) -> str:
    """
    Helper function to POST the service definition to OpenMetadata.
    Returns the Service ID.
    """
    create_url = f"{HOST_URL}/api/v1/services/{category}"
    service_name = payload["name"]

    print(f"   [POST] Creating Service '{service_name}'...")

    try:
        resp = requests.post(create_url, headers=get_headers(), json=payload)

        if resp.status_code in [200, 201]:
            print("   ✅ Service Created.")
            return resp.json()["id"]

        elif resp.status_code == 409:
            print("   ⚠️  Service already exists (409). Fetching ID...")
            get_resp = requests.get(
                f"{create_url}/name/{service_name}", headers=get_headers()
            )
            get_resp.raise_for_status()
            return get_resp.json()["id"]

        else:
            print(f"   ❌ Failed to create service: {resp.status_code} - {resp.text}")
            return None

    except Exception as e:
        print(f"   ❌ Connection Error: {e}")
        return None


def create_standard_service(service_key: str, data: dict):
    """
    Handles standard services (Postgres, Kafka, etc.) that use AutoPilot.
    """
    print(f"\n--- Processing Standard Service: {service_key.upper()} ---")

    # 1. Create the Service
    service_body = data["json"]
    service_id = _create_service_entry(data["category"], service_body)

    if not service_id:
        return

    # 2. Trigger AutoPilot
    trigger_url = f"{HOST_URL}/api/v1/apps/trigger/AutoPilotApplication"
    entity_link = f"<#E::{data['entityType']}::{service_body['name']}>"
    trigger_body = {"entityLink": entity_link}

    print("   [POST] Triggering AutoPilot...")
    try:
        resp = requests.post(trigger_url, headers=get_headers(), json=trigger_body)
        if resp.status_code == 200:
            print("   ✅ AutoPilot Triggered Successfully.")
        else:
            print(f"   ❌ AutoPilot Failed: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"   ❌ Connection Error during trigger: {e}")


def create_custom_pipeline(service_key: str, data: dict):
    """
    Handles custom pipelines (Flink) that require manual ingestion pipeline creation.
    """
    print(f"\n--- Processing Custom Pipeline: {service_key.upper()} ---")

    # 1. Prepare Service Payload (Deep Copy to avoid modifying global DATA)
    service_body = copy.deepcopy(data["json"])

    # Serialize the custom lineage config from Dict -> JSON String
    try:
        conn_opts = service_body["connection"]["config"]["connectionOptions"]
        if "custom_lineage_config" in conn_opts and isinstance(
            conn_opts["custom_lineage_config"], dict
        ):
            conn_opts["custom_lineage_config"] = json.dumps(
                conn_opts["custom_lineage_config"]
            )
    except KeyError:
        pass  # Config might not exist, skip

    # 2. Create the Service
    service_id = _create_service_entry(data["category"], service_body)
    if not service_id:
        return

    # 3. Prepare Ingestion Pipeline Payload
    ingestion_payload = copy.deepcopy(data.get("ingestion_payload"))
    if not ingestion_payload:
        print(
            "   ❌ Error: 'ingestion_payload' missing in payload.py for this service."
        )
        return

    # Inject Dynamic Values (Secrets, IDs, Dates)
    ingestion_payload["service"] = {"id": service_id, "type": "pipelineService"}
    ingestion_payload["airflowConfig"]["startDate"] = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    ingestion_payload["sourceConfig"]["config"]["source"]["serviceName"] = service_body[
        "name"
    ]

    om_config = ingestion_payload["sourceConfig"]["config"]["workflowConfig"][
        "openMetadataServerConfig"
    ]
    om_config["hostPort"] = f"{HOST_URL}/api"
    om_config["securityConfig"] = {"jwtToken": JWT_TOKEN}

    # 4. Create Ingestion Pipeline
    ingestion_url = f"{HOST_URL}/api/v1/services/ingestionPipelines"
    pipeline_id = None
    pipeline_name = ingestion_payload["name"]

    print(f"   [POST] Creating Ingestion Pipeline '{pipeline_name}'...")
    try:
        resp = requests.post(
            ingestion_url, headers=get_headers(), json=ingestion_payload
        )

        if resp.status_code in [200, 201]:
            print("   ✅ Ingestion Pipeline Created.")
            pipeline_id = resp.json()["id"]
        elif resp.status_code == 409:
            print("   ⚠️  Ingestion Pipeline exists. Fetching ID...")
            get_resp = requests.get(
                f"{ingestion_url}/name/{pipeline_name}", headers=get_headers()
            )
            get_resp.raise_for_status()
            pipeline_id = get_resp.json()["id"]
        else:
            print(f"   ❌ Failed to create ingestion: {resp.status_code} - {resp.text}")
            return
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")
        return

    # 5. Trigger Ingestion Pipeline
    trigger_url = f"{ingestion_url}/{pipeline_id}/trigger"
    print(f"   [POST] Triggering Ingestion '{pipeline_name}'...")
    try:
        resp_trigger = requests.post(trigger_url, headers=get_headers())
        if resp_trigger.status_code == 200:
            print("   ✅ Ingestion Triggered Successfully.")
        else:
            print(
                f"   ❌ Failed to trigger ingestion: {resp_trigger.status_code} - {resp_trigger.text}"
            )
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")


def delete_service(service_key):
    """
    DELETE /api/v1/services/{category}/name/{name}?hardDelete=true&recursive=true
    """
    data = DATA.get(service_key)
    if not data:
        print(f"❌ Error: Service '{service_key}' not found.")
        return

    service_name = data["json"]["name"]
    delete_url = f"{HOST_URL}/api/v1/services/{data['category']}/name/{service_name}"
    params = {"hardDelete": "true", "recursive": "true"}

    print(f"\n--- Deleting: {service_key.upper()} ({service_name}) ---")
    try:
        resp = requests.delete(delete_url, headers=get_headers(), params=params)
        if resp.status_code in [200, 204]:
            print("   ✅ Service (and pipelines) deleted.")
        elif resp.status_code == 404:
            print("   ⚠️  Service not found (already deleted).")
        else:
            print(f"   ❌ Delete failed: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OMT Service Manager")
    parser.add_argument("--action", "-a", required=True, choices=["create", "delete"])
    parser.add_argument("--service", "-s", action="append", help="Services to manage.")
    args = parser.parse_args()

    services_to_run = (
        args.service if (args.service and "*" not in args.service) else DATA.keys()
    )
    # Filter only valid keys
    services_to_run = [s for s in services_to_run if s in DATA]

    for svc_key in services_to_run:
        data = DATA[svc_key]

        if args.action == "delete":
            delete_service(svc_key)

        elif args.action == "create":
            # Dispatch based on strategy
            strategy = data.get("ingestion_strategy", "autopilot")

            if strategy == "standard":
                create_custom_pipeline(svc_key, data)
            else:
                create_standard_service(svc_key, data)
