"""Create Kinesis -> Iceberg pipelines on an Etleap deployment via the External API v2.

Clones an existing template pipeline ("Kinesis test2-stream-001"), reusing its
script and destination/primary-key settings, and creates N pipelines:

    Kinesis test2-stream-001 .. test2-stream-010  ->  source stream test2-stream-01
    Kinesis test2-stream-011 .. test2-stream-020  ->  source stream test2-stream-02
    ...                                                (10 pipelines per stream)

For pipeline number P (1-based):
    name        = "Kinesis test2-stream-{P:03d}"
    dest table  = "test2_stream_{P:03d}"            (matches the name)
    source entity (Kinesis stream) = "test2-stream-{((P-1)//10)+1:02d}"

Pipelines whose name already exists are skipped.

Usage:
    python create_pipelines.py --count 2          # create up to ...002
    python create_pipelines.py --count 500        # all 500 (default)
    python create_pipelines.py --count 5 --dry-run
"""

import argparse
import copy
import json
import os
import sys
import time
from http.cookiejar import DefaultCookiePolicy

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HOST = os.environ.get(
    "ETLEAP_HOST",
    "etleap20260616135442225500000027-1671959891.us-east-1.elb.amazonaws.com",
)
BASE_URL = f"https://{HOST}/api/v2"
CREDENTIALS_PATH = os.environ.get("ETLEAP_CREDENTIALS", "api-credentials")

TEMPLATE_PIPELINE_NAME = "Kinesis test2-stream-001"
STREAMS_PER_PIPELINE = 10  # 10 pipelines consume from each Kinesis stream


class RateLimiter:
    """Ensure calls are spaced at least 1/rate seconds apart (so no more than `rate` per second).

    Creating a Kinesis-source pipeline triggers a server-side Kinesis ListStreams call, which
    AWS limits to 5 transactions/second. Throttling the create requests keeps us under that.
    """

    def __init__(self, rate_per_second: float):
        self.min_interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


def load_auth(path: str):
    with open(path) as f:
        creds = json.load(f)
    return (creds["accessKey"], creds["secretKey"])


def pipeline_name(p: int) -> str:
    return f"Kinesis test2-stream-{p:03d}"


def dest_table(p: int) -> str:
    return f"test2_stream_{p:03d}"


def source_stream(p: int) -> str:
    stream_index = (p - 1) // STREAMS_PER_PIPELINE + 1  # 1 -> 1, 10 -> 1, 11 -> 2 ...
    return f"test2-stream-{stream_index:02d}"


def list_existing(session: requests.Session) -> dict:
    """Return {pipeline_name: {"id": ..., "paused": ...}} for all pipelines on the deployment."""
    resp = session.get(f"{BASE_URL}/pipelines", params={"pageSize": 0})
    resp.raise_for_status()
    body = resp.json()
    # The list may come back as a bare list or wrapped under a common key.
    if isinstance(body, list):
        items = body
    else:
        items = next(
            (body[k] for k in ("pipelines", "data", "items", "results") if k in body),
            [],
        )
    return {item["name"]: {"id": item["id"], "paused": item.get("paused")} for item in items}


def get_template(session: requests.Session, template_id: str) -> dict:
    resp = session.get(f"{BASE_URL}/pipelines/{template_id}")
    resp.raise_for_status()
    return resp.json()


def get_script(session: requests.Session, pipeline_id: str, version: int) -> dict:
    resp = session.get(f"{BASE_URL}/pipelines/{pipeline_id}/scripts/{version}")
    resp.raise_for_status()
    return resp.json()


def refresh_pipeline(session: requests.Session, pipeline_id: str) -> requests.Response:
    # POST /pipelines/{id}/refreshes takes no body.
    return session.post(f"{BASE_URL}/pipelines/{pipeline_id}/refreshes")


def is_running(p: int, running_per_stream: int) -> bool:
    """The first `running_per_stream` pipelines within each stream's group of 10 run; rest are paused.

    e.g. running_per_stream=1 -> 001, 011, 021, ... run; all others paused.
    """
    position_in_stream = (p - 1) % STREAMS_PER_PIPELINE + 1  # 1..10
    return position_in_stream <= running_per_stream


def build_body(template: dict, script: dict, p: int, running_per_stream: int) -> dict:
    """Clone the template's source/destination/script, overriding only the per-pipeline fields."""
    source = copy.deepcopy(template["source"])
    source["entity"] = source_stream(p)

    # GET returns destinations[].destination; POST expects a single `destination`.
    dest = copy.deepcopy(template["destinations"][0]["destination"])
    dest["table"] = dest_table(p)

    body = {
        "name": pipeline_name(p),
        "source": source,
        "destination": dest,
        "script": script,
        "paused": not is_running(p, running_per_stream),
    }
    if template.get("parsingErrorSettings"):
        body["parsingErrorSettings"] = template["parsingErrorSettings"]
    return body


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Kinesis->Iceberg pipelines on Etleap")
    parser.add_argument(
        "--count",
        type=int,
        default=500,
        help="Highest pipeline number to create, 1-based (e.g. 2 => up to ...002). Default 500.",
    )
    parser.add_argument(
        "--running-per-stream",
        type=int,
        default=STREAMS_PER_PIPELINE,
        help=(
            f"How many of each stream's {STREAMS_PER_PIPELINE} pipelines are created running; the rest are "
            "paused. e.g. 1 => 001, 011, 021 ... run, others paused. Default: all running."
        ),
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.3,
        help=(
            "Max pipeline-creation requests per second. Each Kinesis pipeline creation triggers a "
            "server-side Kinesis ListStreams call, which AWS limits to 5/s. Default: 5."
        ),
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="After create/update, start a refresh for every pipeline meant to be running (within --count).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be created; make no changes")
    args = parser.parse_args()

    if args.count < 1:
        print("ERROR: --count must be at least 1", file=sys.stderr)
        sys.exit(1)
    if not 0 <= args.running_per_stream <= STREAMS_PER_PIPELINE:
        print(f"ERROR: --running-per-stream must be between 0 and {STREAMS_PER_PIPELINE}", file=sys.stderr)
        sys.exit(1)
    if args.rate <= 0:
        print("ERROR: --rate must be greater than 0", file=sys.stderr)
        sys.exit(1)

    create_limiter = RateLimiter(args.rate)

    session = requests.Session()
    session.auth = load_auth(CREDENTIALS_PATH)
    session.verify = False  # ELB self-signed cert
    session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
    # The API rejects requests carrying both an auth header and a session cookie, so
    # refuse all cookies and authenticate with Basic auth on every request.
    session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))

    existing = list_existing(session)
    if TEMPLATE_PIPELINE_NAME not in existing:
        print(f"ERROR: template pipeline {TEMPLATE_PIPELINE_NAME!r} not found on deployment", file=sys.stderr)
        sys.exit(1)

    template_id = existing[TEMPLATE_PIPELINE_NAME]["id"]
    template = get_template(session, template_id)
    script = get_script(session, template_id, template["latestScriptVersion"])
    print(
        f"Template {TEMPLATE_PIPELINE_NAME!r} (id {template_id}): "
        f"source conn {template['source']['connectionId']}, "
        f"dest conn {template['destinations'][0]['destination']['connectionId']}, "
        f"primaryKey {template['destinations'][0]['destination'].get('primaryKey')}"
    )

    created = updated = unchanged = failed = 0
    # Pipelines meant to be running, that exist (or were just created): (name, id) to refresh.
    to_refresh = []
    for p in range(1, args.count + 1):
        name = pipeline_name(p)
        should_pause = not is_running(p, args.running_per_stream)
        state = "paused" if should_pause else "running"

        if name in existing:
            # Existing pipeline: the only allowed modification is the paused flag. Sync it.
            if not should_pause:
                to_refresh.append((name, existing[name]["id"]))
            current_paused = existing[name]["paused"]
            if current_paused == should_pause:
                print(f"OK     {name} (already {state})")
                unchanged += 1
                continue
            if args.dry_run:
                print(f"DRYRUN {name}  PATCH paused={should_pause} ({'pause' if should_pause else 'unpause'})")
                updated += 1
                continue
            resp = session.patch(f"{BASE_URL}/pipelines/{existing[name]['id']}", data=json.dumps({"paused": should_pause}))
            if resp.status_code in (200, 201):
                print(f"UPDATE {name}  -> {state}")
                updated += 1
            else:
                print(f"FAIL   {name}  PATCH HTTP {resp.status_code}: {resp.text[:500]}", file=sys.stderr)
                failed += 1
            continue

        body = build_body(template, script, p, args.running_per_stream)
        if args.dry_run:
            print(f"DRYRUN {name}  source={body['source']['entity']}  table={body['destination']['table']}  {state}")
            created += 1
            if not should_pause:
                to_refresh.append((name, None))  # id unknown in dry-run
            continue

        create_limiter.wait()  # stay under the Kinesis ListStreams rate limit
        resp = session.post(f"{BASE_URL}/pipelines", data=json.dumps(body))
        if resp.status_code in (200, 201):
            new_id = resp.json().get("id", "?")
            print(f"CREATE {name}  source={body['source']['entity']}  table={body['destination']['table']}  {state}  id={new_id}")
            created += 1
            if not should_pause:
                to_refresh.append((name, new_id))
        else:
            print(f"FAIL   {name}  HTTP {resp.status_code}: {resp.text[:500]}", file=sys.stderr)
            failed += 1

    print(f"\nDone. created={created} updated={updated} unchanged={unchanged} failed={failed}")

    refreshed = refresh_failed = 0
    if args.refresh:
        print(f"\nRefreshing {len(to_refresh)} running pipeline(s)...")
        for name, pid in to_refresh:
            if args.dry_run:
                print(f"DRYRUN {name}  POST refresh")
                refreshed += 1
                continue
            resp = refresh_pipeline(session, pid)
            if resp.status_code in (200, 201):
                print(f"REFRESH {name}")
                refreshed += 1
            else:
                print(f"FAIL    {name}  refresh HTTP {resp.status_code}: {resp.text[:500]}", file=sys.stderr)
                refresh_failed += 1
        print(f"\nRefresh done. refreshed={refreshed} failed={refresh_failed}")

    if failed or refresh_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
