import gzip
import base64
import io
import json
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import migration


def archive(*events):
    data = b"".join(json.dumps(e).encode() + b"\n" for e in events)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as z:
        z.writestr("events.json.gz", gzip.compress(data))
    return output.getvalue()


class MigrationTests(unittest.TestCase):
    def test_convert_preserves_event_identity_and_time(self):
        raw = {
            "event_type": "SubscriptionCompleted", "event_time": "2026-10-01T12:30:00Z",
            "user_id": "client-123", "device_id": "device-123", "$insert_id": "original",
            "platform": "Web", "event_properties": {"kind": "monthly"},
            "user_properties": {"plan": "basic"}, "session_id": 42,
            "amplitude_id": 999,
        }
        result = migration.convert_event(raw, "web")
        self.assertEqual(result["time"], 1790857800000)
        self.assertEqual(result["insert_id"], "web:original")
        self.assertEqual(result["platform"], "Web")
        self.assertEqual(result["event_properties"], {"kind": "monthly"})
        self.assertTrue(result["$skip_user_properties_sync"])
        self.assertNotIn("amplitude_id", result)

    def test_archive_filters_on_event_time_not_upload_time(self):
        payload = archive(
            {"event_type": "A", "event_time": "2026-10-01T10:00:00Z", "user_id": "client-1", "uuid": "a"},
            {"event_type": "B", "event_time": "2026-10-02T10:00:00Z", "user_id": "client-1", "uuid": "b"},
        )
        start = migration.parse_time("2026-10-01T00:00:00Z")
        end = migration.parse_time("2026-10-02T00:00:00Z")
        events = list(migration.events_from_archive(payload, start, end, "web"))
        self.assertEqual([e["event_type"] for e in events], ["A"])

    def test_export_timestamp_without_zone_is_interpreted_as_utc(self):
        raw = {"event_type": "A", "event_time": "2026-10-01 12:30:00.000000",
               "user_id": "client-123", "uuid": "a"}
        self.assertEqual(migration.convert_event(raw, "web")["time"], 1790857800000)

    def test_broken_archive_is_rejected_before_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = migration.parse_time("2026-10-01T00:00:00Z")
            end = migration.parse_time("2026-10-01T01:00:00Z")
            settings = migration.Settings("sourcekey", "secret", "targetkey", "web", start, end,
                                          start, end, "us", Path(tmp) / "checkpoint.json", 10)
            class FakeClient:
                def export_hour(self, hour):
                    return b"not a zip"
                def upload(self, events):
                    self.fail("upload must not run")
            with patch.object(migration, "Client", return_value=FakeClient()):
                with self.assertRaises(zipfile.BadZipFile):
                    migration.run(settings)

    def test_hourly_pipeline_resumes_without_replaying_completed_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = migration.parse_time("2026-10-01T00:00:00Z")
            end = migration.parse_time("2026-10-01T02:00:00Z")
            settings = migration.Settings("sourcekey", "secret", "targetkey", "web", start, end,
                                          start, end, "us", Path(tmp) / "checkpoint.json", 10)
            seen = []

            class FakeClient:
                def export_hour(self, hour):
                    seen.append(("export", hour.hour))
                    return archive({"event_type": "A", "event_time": hour.isoformat(),
                                    "user_id": "client-1", "uuid": str(hour.hour)})

                def upload(self, events):
                    seen.append(("upload", len(events)))

            with patch.object(migration, "Client", return_value=FakeClient()):
                migration.run(settings)
                migration.run(settings)
            self.assertEqual(seen, [("export", 0), ("upload", 1),
                                    ("export", 1), ("upload", 1)])
            checkpoint = settings.checkpoint.read_text()
            self.assertNotIn("sourcekey", checkpoint)
            self.assertNotIn("secret", checkpoint)
            self.assertNotIn("targetkey", checkpoint)

    def test_client_exports_and_uploads_with_expected_credentials_and_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = migration.parse_time("2026-10-01T00:00:00Z")
            settings = migration.Settings("sourcekey", "secret", "targetkey", "web", start,
                                          start + migration.HOUR, start, start + migration.HOUR,
                                          "us", Path(tmp) / "checkpoint.json", 10)
            requests = []
            data = archive({"event_type": "A", "event_time": start.isoformat(),
                            "user_id": "client-1", "uuid": "a"})

            class Response(io.BytesIO):
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    self.close()

            def respond(request, timeout):
                requests.append(request)
                if request.get_method() == "GET":
                    return Response(data)
                return Response(b'{"code":200,"events_ingested":1}')

            with patch.object(migration.urllib.request, "urlopen", side_effect=respond):
                client = migration.Client(settings)
                path = client.export_hour(start)
                try:
                    events = list(migration.events_from_archive(path, start, start + migration.HOUR, "web"))
                    client.upload(events)
                finally:
                    path.unlink(missing_ok=True)

            self.assertIn("start=20261001T00&end=20261001T00", requests[0].full_url)
            self.assertEqual(requests[0].get_header("Authorization"),
                             "Basic " + base64.b64encode(b"sourcekey:secret").decode())
            self.assertEqual(requests[1].full_url, "https://api2.amplitude.com/batch")
            body = json.loads(requests[1].data)
            self.assertEqual(body["api_key"], "targetkey")
            self.assertEqual(body["events"][0]["insert_id"], "web:a")

    def test_client_exports_multi_hour_window_with_inclusive_end_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = migration.parse_time("2026-10-01T00:00:00Z")
            settings = migration.Settings("sourcekey", "secret", "targetkey", "source1", start,
                                          start + 24 * migration.HOUR, start,
                                          start + 24 * migration.HOUR, "us",
                                          Path(tmp) / "checkpoint.json", 10, 24)
            requested = []

            class Response(io.BytesIO):
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    self.close()

            def respond(request, timeout):
                requested.append(request.full_url)
                return Response(archive({"event_type": "A", "event_time": start.isoformat(),
                                         "user_id": "client-1", "uuid": "a"}))

            with patch.object(migration.urllib.request, "urlopen", side_effect=respond):
                path = migration.Client(settings).export_range(start, start + 24 * migration.HOUR)
                path.unlink()
            self.assertIn("start=20261001T00&end=20261001T23", requested[0])

    def test_interrupted_export_removes_temporary_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = migration.parse_time("2026-10-01T00:00:00Z")
            settings = migration.Settings("sourcekey", "secret", "targetkey", "source1", start,
                                          start + migration.HOUR, start, start + migration.HOUR,
                                          "us", Path(tmp) / "checkpoint.json", 10)
            with patch.object(migration.urllib.request, "urlopen", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    migration.Client(settings).export_hour(start)
            self.assertEqual(list(Path(tmp).glob("amplitude-export-*.zip")), [])


if __name__ == "__main__":
    unittest.main()
