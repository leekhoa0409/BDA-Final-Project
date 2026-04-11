"""
NiFi Weather Flow Creator

Creates a NiFi flow that fetches current weather data from OpenWeather API
for Vietnamese cities and publishes to Kafka topic 'weather-raw'.

Usage:
    python create-weather-flow.py [--nifi-url https://localhost:8443] [--api-key YOUR_KEY]

Prerequisites:
    - NiFi running at https://localhost:8443
    - Kafka running at kafka:9092
    - pip install requests urllib3
"""

import argparse
import os
from urllib.parse import quote
import requests
import urllib3

# Suppress SSL warnings for self-signed NiFi certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Target city for streaming weather data (API 2.5 - free tier)
# Free tier: 60 calls/min, 1,000,000 calls/month
TARGET_CITIES = [
    {"name": "New York", "city_query": "New York,US"},
]


class NiFiFlowCreator:
    def __init__(self, nifi_url, username="admin", password="admin123456789"):
        self.nifi_url = nifi_url.rstrip("/")
        self.api_url = f"{self.nifi_url}/nifi-api"
        self.session = requests.Session()
        self.session.verify = False
        self.token = None
        self._authenticate(username, password)

    def _authenticate(self, username, password):
        resp = self.session.post(
            f"{self.api_url}/access/token",
            data={"username": username, "password": password},
        )
        if resp.status_code == 201:
            self.token = resp.text
            self.session.headers["Authorization"] = f"Bearer {self.token}"
            print(f"Authenticated with NiFi at {self.nifi_url}")
        else:
            raise Exception(f"NiFi authentication failed: {resp.status_code} {resp.text}")

    def get_root_process_group(self):
        resp = self.session.get(f"{self.api_url}/flow/process-groups/root")
        resp.raise_for_status()
        return resp.json()["processGroupFlow"]["id"]

    def create_process_group(self, parent_id, name, x=0, y=0):
        body = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": x, "y": y},
            },
        }
        resp = self.session.post(
            f"{self.api_url}/process-groups/{parent_id}/process-groups",
            json=body,
        )
        resp.raise_for_status()
        pg = resp.json()
        print(f"Created process group: {name} (id={pg['id']})")
        return pg["id"]

    def create_processor(self, group_id, proc_type, name, config, x=0, y=0,
                         auto_terminated_relationships=None):
        component = {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
            "config": config,
        }
        if auto_terminated_relationships:
            component["config"]["autoTerminatedRelationships"] = auto_terminated_relationships
        body = {
            "revision": {"version": 0},
            "component": component,
        }
        resp = self.session.post(
            f"{self.api_url}/process-groups/{group_id}/processors",
            json=body,
        )
        resp.raise_for_status()
        proc = resp.json()
        print(f"Created processor: {name} (id={proc['id']})")
        return proc["id"], proc["revision"]["version"]

    def create_connection(self, group_id, source_id, dest_id, relationships):
        body = {
            "revision": {"version": 0},
            "component": {
                "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
                "destination": {"id": dest_id, "groupId": group_id, "type": "PROCESSOR"},
                "selectedRelationships": relationships,
            },
        }
        resp = self.session.post(
            f"{self.api_url}/process-groups/{group_id}/connections",
            json=body,
        )
        resp.raise_for_status()
        print(f"Created connection: {source_id} -> {dest_id} ({relationships})")

    def create_weather_flow(self, api_key):
        root_id = self.get_root_process_group()
        pg_id = self.create_process_group(root_id, "OpenWeather One Call to Kafka", x=100, y=100)

        # PublishKafka processor (shared by all city InvokeHTTP processors)
        # Auto-terminate 'success' since successfully published messages need no further routing
        kafka_id, _ = self.create_processor(
            pg_id,
            "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
            "Publish to Kafka",
            {
                "schedulingPeriod": "0 sec",
                "properties": {
                    "bootstrap.servers": "kafka:9092",
                    "topic": "weather-raw",
                    "acks": "1",
                    "use-transactions": "false",
                },
            },
            x=400, y=100,
            auto_terminated_relationships=["success"],
        )

        # LogAttribute for failures — auto-terminate 'success' after logging
        log_id, _ = self.create_processor(
            pg_id,
            "org.apache.nifi.processors.standard.LogAttribute",
            "Log Failures",
            {"schedulingPeriod": "0 sec"},
            x=700, y=100,
            auto_terminated_relationships=["success"],
        )

        # Create one GenerateFlowFile + InvokeHTTP pair per city
        for i, city in enumerate(TARGET_CITIES):
            encoded_query = quote(city["city_query"], safe="")
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?q={encoded_query}"
                f"&units=metric"
                f"&appid={api_key}"
            )

            # GenerateFlowFile triggers InvokeHTTP on schedule
            gen_id, _ = self.create_processor(
                pg_id,
                "org.apache.nifi.processors.standard.GenerateFlowFile",
                f"Trigger {city['name']}",
                {
                    "schedulingPeriod": "5 min",
                    "properties": {
                        "File Size": "0B",
                    },
                },
                x=100, y=300 + i * 200,
            )

            # InvokeHTTP fetches weather data — auto-terminate 'Original' relationship
            invoke_id, _ = self.create_processor(
                pg_id,
                "org.apache.nifi.processors.standard.InvokeHTTP",
                f"Fetch {city['name']}",
                {
                    "schedulingPeriod": "0 sec",
                    "properties": {
                        "HTTP Method": "GET",
                        "Remote URL": url,
                    },
                },
                x=400, y=300 + i * 200,
                auto_terminated_relationships=["Original"],
            )

            # GenerateFlowFile → InvokeHTTP
            self.create_connection(pg_id, gen_id, invoke_id, ["success"])
            # InvokeHTTP → Kafka (Response) and Log (failures)
            self.create_connection(pg_id, invoke_id, kafka_id, ["Response"])
            self.create_connection(pg_id, invoke_id, log_id, ["Failure", "No Retry", "Retry"])

        self.create_connection(pg_id, kafka_id, log_id, ["failure"])

        city_names = [c["name"] for c in TARGET_CITIES]
        print("\nWeather flow created successfully!")
        print(f"Process Group: OpenWeather One Call to Kafka (id={pg_id})")
        print(f"Cities ({len(TARGET_CITIES)}): {city_names}")
        print("API: Current Weather 2.5 (city query, units=metric, free tier)")
        print("Schedule: every 5 minutes per city")
        print("Kafka topic: weather-raw")
        print("\nNote: You need to start the processors manually from the NiFi UI.")
        return pg_id


def main():
    parser = argparse.ArgumentParser(description="Create NiFi weather flow")
    parser.add_argument("--nifi-url", default="https://localhost:8443",
                        help="NiFi URL (default: https://localhost:8443)")
    parser.add_argument("--api-key", default=None,
                        help="OpenWeather API key (or set OPENWEATHER_API_KEY env var)")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("OPENWEATHER_API_KEY")
    if not api_key or api_key == "your_api_key_here":
        print("ERROR: Please provide a valid OpenWeather API key")
        print("  --api-key YOUR_KEY  or  export OPENWEATHER_API_KEY=YOUR_KEY")
        return

    creator = NiFiFlowCreator(args.nifi_url)
    creator.create_weather_flow(api_key)


if __name__ == "__main__":
    main()
