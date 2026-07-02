import argparse
import json
import os
import random
import sys
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3


def uuid7() -> uuid.UUID:
    """Return a UUIDv7. Uses the stdlib (Python 3.14+) when available, else builds one."""
    if hasattr(uuid, "uuid7"):
        return uuid.uuid7()

    # Manual UUIDv7: 48-bit Unix timestamp (ms) + version/variant + random bits.
    ts_ms = time.time_ns() // 1_000_000
    rand_a = random.getrandbits(12)
    rand_b = random.getrandbits(62)
    value = (ts_ms & 0xFFFFFFFFFFFF) << 80
    value |= 0x7 << 76          # version 7
    value |= rand_a << 64
    value |= 0b10 << 62         # variant
    value |= rand_b
    return uuid.UUID(int=value)

# Streams are named "<prefix>-NN" (e.g. test2-stream-01), matching the Terraform
# in test-vpc/kinesis.tf which creates them via format("test2-stream-%02d", i).
KINESIS_STREAM_PREFIX = os.environ.get("KINESIS_STREAM_PREFIX", "test2-stream")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
KINESIS_ENDPOINT = os.environ.get("KINESIS_ENDPOINT_URL")

# 2026 FIFA World Cup participants (48 teams; England/Scotland share ISO code GB — GB used for England)
COUNTRY_CODES = [
    # Hosts (CONCACAF)
    "US", "CA", "MX",
    # CONCACAF (non-hosts)
    "PA", "CW", "HT",
    # CONMEBOL
    "AR", "BR", "CO", "UY", "EC", "PY",
    # UEFA (Scotland has no ISO alpha-2; using "SC" — Seychelles code, not in tournament)
    "DE", "ES", "FR", "PT", "NL", "GB", "HR", "AT",
    "CH", "TR", "BE", "BA", "SE", "CZ", "NO", "SC",
    # AFC
    "JP", "KR", "IR", "AU", "SA", "IQ", "JO", "UZ", "QA",
    # CAF
    "MA", "SN", "EG", "GH", "TN", "DZ", "CI", "CV", "ZA", "CD",
    # OFC
    "NZ",
]

# Relative weights approximating each country's internet-user population
COUNTRY_WEIGHTS = [
    # US   CA   MX
    300,   50,  80,
    # PA   CW   HT
      8,    3,   5,
    # AR   BR   CO   UY   EC   PY
     30,  150,  40,   5,  10,   8,
    # DE   ES   FR   PT   NL   GB   HR   AT
     80,   50,  70,  20,  20,  80,   5,  20,
    # CH   TR   BE   BA   SE   CZ   NO  SCO
     20,   60,  20,   8,  25,  15,  12,  12,
    # JP   KR   IR   AU   SA   IQ   JO   UZ   QA
    120,   70,  30,  40,  30,  15,   8,   8,  10,
    # MA   SN   EG   GH   TN   DZ   CI   CV   ZA   CD
     20,    8,  30,  15,  10,  20,   8,   3,  30,  25,
    # NZ
     10,
]

BATCH_SIZE = 500  # Kinesis put_records hard max is 500 records per call
MAX_RECORDS_PER_SECOND = 1000  # per-shard write limit; each stream has 1 shard

_print_lock = threading.Lock()


def log(message: str) -> None:
    # Workers run in separate processes (own GIL); flush so lines appear promptly and
    # are written atomically enough that infrequent progress lines don't tear.
    with _print_lock:
        print(message, flush=True)


def make_record() -> dict:
    return {
        "countryCode": random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS, k=1)[0],
        "voteId": str(uuid7()),
        "voteTime": datetime.now(tz=timezone.utc).isoformat(timespec="microseconds"),
    }


def stream_name(index: int) -> str:
    # index is 1-based; matches format("test2-stream-%02d", i) in Terraform
    return f"{KINESIS_STREAM_PREFIX}-{index:02d}"


def make_client():
    # One client per thread: boto3 clients are thread-safe, but giving each worker its
    # own client avoids any shared-session contention under concurrency.
    kinesis_kwargs: dict = dict(region_name=AWS_REGION)
    if KINESIS_ENDPOINT:
        kinesis_kwargs["endpoint_url"] = KINESIS_ENDPOINT
    return boto3.client("kinesis", **kinesis_kwargs)


def put_with_retries(kinesis, target_stream: str, records: list, max_attempts: int = 5) -> None:
    """put_records, retrying only the records that failed (e.g. ProvisionedThroughputExceeded)."""
    pending = records
    for attempt in range(1, max_attempts + 1):
        response = kinesis.put_records(StreamName=target_stream, Records=pending)
        if not response.get("FailedRecordCount"):
            return
        # Keep just the entries that failed, by index, and back off before retrying.
        results = response["Records"]
        pending = [rec for rec, res in zip(pending, results) if res.get("ErrorCode")]
        if attempt == max_attempts:
            raise RuntimeError(
                f"{len(pending)} records still failing on {target_stream} after {max_attempts} attempts "
                f"(last error: {results[0].get('ErrorCode')})"
            )
        time.sleep(0.1 * attempt)


def write_stream(target_stream: str, count: int) -> int:
    """Write `count` records to a single stream, self-throttled to < MAX_RECORDS_PER_SECOND."""
    kinesis = make_client()
    written = 0
    # Log progress as each 25% milestone is crossed.
    milestones = [int(count * frac) for frac in (0.25, 0.50, 0.75, 1.00)]
    next_milestone = 0
    while written < count:
        batch = [make_record() for _ in range(min(BATCH_SIZE, count - written))]
        records = [
            {"Data": json.dumps(r).encode("utf-8"), "PartitionKey": str(uuid7())}
            for r in batch
        ]

        batch_start = time.monotonic()
        put_with_retries(kinesis, target_stream, records)
        written += len(batch)

        while next_milestone < len(milestones) and written >= milestones[next_milestone]:
            pct = (next_milestone + 1) * 25
            log(f"{target_stream}: {pct}% ({written}/{count} records)")
            next_milestone += 1

        # Stay under MAX_RECORDS_PER_SECOND for THIS stream.
        elapsed = time.monotonic() - batch_start
        min_time = len(batch) / MAX_RECORDS_PER_SECOND
        if elapsed < min_time:
            time.sleep(min_time - elapsed)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Write vote records to Kinesis")
    parser.add_argument("--count", type=int, required=True, help="Number of records to write to EACH stream")
    parser.add_argument(
        "--streams",
        type=int,
        required=True,
        help="Number of streams to write to; the first N streams "
        "(e.g. 1 writes only to %s)" % stream_name(1),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of concurrent worker processes (default: one per stream). Lower this if "
        "running many streams strains memory; queued streams still run at full rate when started.",
    )
    args = parser.parse_args()

    if args.streams < 1:
        print("ERROR: --streams must be at least 1", file=sys.stderr)
        sys.exit(1)

    stream_names = [stream_name(i) for i in range(1, args.streams + 1)]
    workers = args.workers or len(stream_names)

    log(
        f"Writing {args.count} records to EACH of {len(stream_names)} streams "
        f"({args.count * len(stream_names)} total), up to {MAX_RECORDS_PER_SECOND} rec/s each, "
        f"{workers} worker process(es)"
    )

    failures = []
    # One process per stream (own GIL) so each stream's botocore signing/parsing and throttle
    # run truly in parallel; threads bottlenecked here because the GIL serializes that work.
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(write_stream, name, args.count): name for name in stream_names}
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - report per-stream, keep others running
                log(f"ERROR on {name}: {exc}")
                failures.append(name)

    if failures:
        print(f"\nFailed streams: {', '.join(sorted(failures))}", file=sys.stderr)
        sys.exit(1)
    log(f"\nDone. Wrote {args.count} records to each of {len(stream_names)} streams.")


if __name__ == "__main__":
    main()
