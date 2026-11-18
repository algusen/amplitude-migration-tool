#!/usr/bin/env python3
"""Stream Amplitude Export API hours into another project's Batch API."""

import argparse
import base64
import gzip
import hashlib
import io
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from getpass import getpass
from pathlib import Path


LOG = logging.getLogger("amplitude-migration")
UTC = timezone.utc
HOUR = timedelta(hours=1)
ENDPOINTS = {
    "us": ("https://amplitude.com/api/2/export", "https://api2.amplitude.com/batch"),
    "eu": ("https://analytics.eu.amplitude.com/api/2/export", "https://api.eu.amplitude.com/batch"),
}


def parse_time(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value!r}; use ISO 8601 with a time zone") from exc
    if result.tzinfo is None:
        raise ValueError("Include a time zone, such as +03:00 or Z")
    return result.astimezone(UTC)


def parse_export_time(value: str) -> datetime:
    """Amplitude export timestamps are UTC, including older zone-less values."""
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def ceil_hour(value: datetime) -> datetime:
    rounded = floor_hour(value)
    return rounded if value == rounded else rounded + HOUR


@dataclass(frozen=True)
class Settings:
    source_key: str
    source_secret: str
    destination_key: str
    source_name: str
    start: datetime
    end: datetime
    export_start: datetime
    export_end: datetime  # exclusive
    region: str
    checkpoint: Path
    batch_size: int
    window_hours: int = 1


def convert_event(raw: dict, source_name: str) -> dict:
    if not raw.get("event_type") or not raw.get("event_time"):
        raise ValueError("Event is missing event_type or event_time")
    if not raw.get("user_id") and not raw.get("device_id"):
        raise ValueError("Event is missing both user_id and device_id")
    event_time = parse_export_time(raw["event_time"])
    original_id = raw.get("$insert_id") or raw.get("uuid")
    if not original_id:
        original_id = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
    result = {
        "event_type": raw["event_type"],
        "time": int(event_time.timestamp() * 1000),
        "insert_id": f"{source_name}:{original_id}",
        "$skip_user_properties_sync": True,
    }
    for field in ("user_id", "device_id", "event_properties", "user_properties",
                  "groups", "group_properties", "platform", "os_name", "os_version",
                  "country", "region", "city", "dma", "language", "carrier",
                  "session_id", "event_id", "location_lat", "location_lng"):
        if raw.get(field) is not None:
            result[field] = raw[field]
    if raw.get("version_name") is not None:
        result["app_version"] = raw["version_name"]
    return result


def events_from_archive(data: bytes | Path, start: datetime, end: datetime, source_name: str):
    with zipfile.ZipFile(io.BytesIO(data) if isinstance(data, bytes) else data) as archive:
        for name in archive.namelist():
            if name.endswith("/"):
                continue
            with archive.open(name) as member:
                stream = gzip.GzipFile(fileobj=member) if name.endswith(".gz") else member
                with stream:
                    for line_number, line in enumerate(stream, 1):
                        if not line.strip():
                            continue
                        try:
                            raw = json.loads(line)
                            event_time = parse_export_time(raw["event_time"])
                            if start <= event_time < end:
                                yield convert_event(raw, source_name)
                        except (KeyError, ValueError, TypeError) as exc:
                            raise ValueError(f"Invalid event in {name}, line {line_number}: {exc}") from exc


class Client:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.export_url, self.batch_url = ENDPOINTS[settings.region]

    def _request(self, request: urllib.request.Request, *, missing_is_empty=False,
                 output_path: Path | None = None) -> bytes | Path | None:
        for attempt in range(6):
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    if output_path:
                        with output_path.open("wb") as output:
                            os.chmod(output_path, 0o600)
                            shutil.copyfileobj(response, output)
                        return output_path
                    return response.read()
            except urllib.error.HTTPError as exc:
                if exc.code == 404 and missing_is_empty:
                    return None
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 5:
                    detail = exc.read(500).decode("utf-8", "replace")
                    for secret in (self.settings.source_key, self.settings.source_secret,
                                   self.settings.destination_key):
                        detail = detail.replace(secret, "[redacted]")
                    raise RuntimeError(f"Amplitude HTTP {exc.code}: {detail}") from exc
                delay = min(2 ** attempt * 5, 60)
                LOG.warning("HTTP %s; retrying in %s seconds", exc.code, delay)
                time.sleep(delay)
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt == 5:
                    raise RuntimeError("Network error after 6 attempts") from exc
                delay = min(2 ** attempt * 5, 60)
                LOG.warning("Network error; retrying in %s seconds", delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    def export_range(self, start: datetime, end: datetime) -> Path | None:
        query = urllib.parse.urlencode({"start": start.strftime("%Y%m%dT%H"),
                                        "end": (end - HOUR).strftime("%Y%m%dT%H")})
        auth = base64.b64encode(f"{self.settings.source_key}:{self.settings.source_secret}".encode()).decode()
        request = urllib.request.Request(f"{self.export_url}?{query}",
                                         headers={"Authorization": f"Basic {auth}"})
        self.settings.checkpoint.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="amplitude-export-", suffix=".zip",
                                         dir=self.settings.checkpoint.parent, delete=False) as temp:
            path = Path(temp.name)
        try:
            result = self._request(request, missing_is_empty=True, output_path=path)
            if result is None:
                path.unlink(missing_ok=True)
            return result
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    def export_hour(self, hour: datetime) -> Path | None:
        return self.export_range(hour, hour + HOUR)

    def upload(self, events: list[dict]) -> None:
        payload = json.dumps({"api_key": self.settings.destination_key, "events": events,
                              "options": {"min_id_length": 1}}, separators=(",", ":")).encode()
        if len(payload) > 19_000_000:
            raise ValueError("Batch exceeds 19 MB; reduce --batch-size")
        request = urllib.request.Request(self.batch_url, data=payload,
                                         headers={"Content-Type": "application/json"}, method="POST")
        response = json.loads(self._request(request))
        if response.get("code") != 200 or response.get("events_ingested") != len(events):
            raise RuntimeError(f"Incomplete upload confirmation: code {response.get('code')}, "
                               f"accepted {response.get('events_ingested')} of {len(events)} events")


def checkpoint_identity(settings: Settings) -> str:
    values = [settings.source_key, settings.destination_key, settings.source_name,
              settings.start.isoformat(), settings.end.isoformat(),
              settings.export_start.isoformat(), settings.export_end.isoformat(), settings.region]
    return hashlib.sha256("\0".join(values).encode()).hexdigest()


def save_checkpoint(path: Path, identity: str, completed: list[str], uploaded: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"identity": identity, "completed_hours": completed,
                                     "uploaded_events": uploaded}, indent=2), encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def run(settings: Settings) -> int:
    if settings.start >= settings.end or settings.export_start >= settings.export_end:
        raise ValueError("The period start must be before its end")
    if not 1 <= settings.batch_size <= 2000:
        raise ValueError("--batch-size must be between 1 and 2000")
    if not 1 <= settings.window_hours <= 24:
        raise ValueError("--window-hours must be between 1 and 24")
    if settings.source_key == settings.destination_key:
        raise ValueError("Source and destination project API keys are identical")
    identity = checkpoint_identity(settings)
    if settings.checkpoint.exists():
        state = json.loads(settings.checkpoint.read_text(encoding="utf-8"))
        if state.get("identity") != identity:
            raise ValueError("Checkpoint belongs to a different migration")
    else:
        state = {"completed_hours": [], "uploaded_events": 0}
    completed = state["completed_hours"]
    uploaded = state["uploaded_events"]
    client = Client(settings)
    total_hours = int((settings.export_end - settings.export_start) / HOUR)
    LOG.info("Event period: %s to %s; export window: %s to %s (%s hours)",
             settings.start.isoformat(), settings.end.isoformat(),
             settings.export_start.isoformat(), settings.export_end.isoformat(), total_hours)
    hour = settings.export_start
    while hour < settings.export_end:
        stamp = hour.isoformat()
        if stamp in completed:
            hour += HOUR
            continue
        window_end = min(hour + settings.window_hours * HOUR, settings.export_end)
        LOG.info("Exporting %s to %s", stamp, window_end.isoformat())
        archive = (client.export_hour(hour) if settings.window_hours == 1
                   else client.export_range(hour, window_end))
        count = 0
        try:
            if archive:
                # Validate the full archive before the first write to the target project.
                LOG.info("Validating and converting window %s", stamp)
                selected = sum(1 for _ in events_from_archive(archive, settings.start,
                                                              settings.end, settings.source_name))
                LOG.info("Found %s events; uploading", selected)
                batch = []
                for event in events_from_archive(archive, settings.start, settings.end,
                                                 settings.source_name):
                    batch.append(event)
                    if len(batch) >= settings.batch_size:
                        client.upload(batch)
                        count += len(batch)
                        LOG.info("Uploaded in current window: %s of %s", count, selected)
                        batch = []
                if batch:
                    client.upload(batch)
                    count += len(batch)
        finally:
            if isinstance(archive, Path):
                archive.unlink(missing_ok=True)
        uploaded += count
        next_hour = hour
        while next_hour < window_end:
            completed.append(next_hour.isoformat())
            next_hour += HOUR
        save_checkpoint(settings.checkpoint, identity, completed, uploaded)
        LOG.info("Window complete: %s events; %s total", count, uploaded)
        hour = window_end
    LOG.info("Migration complete: %s events submitted", uploaded)
    return uploaded


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="start", required=True,
                        help="Event period start, ISO 8601 with a time zone")
    parser.add_argument("--to", dest="end", required=True,
                        help="Event period end (exclusive)")
    parser.add_argument("--source-name", required=True, help="Unique source name, such as web or mobile")
    parser.add_argument("--region", choices=ENDPOINTS, default="us")
    parser.add_argument("--late-arrival-hours", type=int, default=48,
                        help="Extra hours to scan for late uploads (default: 48)")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--window-hours", type=int, default=24,
                        help="Hours per export request (default: 24)")
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--source-api-key")
    parser.add_argument("--source-secret-key")
    parser.add_argument("--destination-api-key")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    try:
        start, end = parse_time(args.start), parse_time(args.end)
        if start >= end:
            raise ValueError("--from must be earlier than --to")
        if args.late_arrival_hours < 0:
            raise ValueError("--late-arrival-hours cannot be negative")
        export_start = floor_hour(start)
        export_end = ceil_hour(end + timedelta(hours=args.late_arrival_hours))
        latest_available = floor_hour(datetime.now(UTC) - timedelta(hours=2))
        if export_end > latest_available:
            raise ValueError("The full export window is not available yet: shorten the period or "
                             "late-arrival allowance, or wait for data (Export API has a delay of up to 2 hours)")
        source_key = args.source_api_key or os.getenv("AMPLITUDE_SOURCE_API_KEY") or getpass("Source API key: ")
        source_secret = args.source_secret_key or os.getenv("AMPLITUDE_SOURCE_SECRET_KEY") or getpass("Source secret key: ")
        destination_key = (args.destination_api_key or os.getenv("AMPLITUDE_DESTINATION_API_KEY")
                           or getpass("Destination API key: "))
        if not all((source_key, source_secret, destination_key)):
            raise ValueError("All three keys are required")
        if source_key == destination_key:
            raise ValueError("Source and destination API keys are identical")
        fingerprint = hashlib.sha256(f"{source_key}:{destination_key}:{args.source_name}:{start}:{end}".encode()).hexdigest()[:12]
        checkpoint = args.checkpoint or Path(__file__).parent / ".migration-state" / f"{fingerprint}.json"
        settings = Settings(source_key, source_secret, destination_key, args.source_name,
                            start, end, export_start, export_end, args.region, checkpoint,
                            args.batch_size, args.window_hours)
        run(settings)
        return 0
    except (ValueError, RuntimeError, OSError, zipfile.BadZipFile) as exc:
        LOG.error("Migration stopped: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
