# Amplitude Migration Tool

A command-line tool for migrating historical events from one Amplitude project to another over a specified period. It downloads events through the [Export API](https://amplitude.com/docs/apis/analytics/export), selects them by event time, and sends them to the destination project through the [Batch Event Upload API](https://amplitude.com/docs/apis/analytics/batch-event-upload). The source project is not modified.

One use case is consolidating web and mobile events in a single Amplitude project. Run the tool once for each source project. It migrates events, not the complete project configuration.

## Requirements

- Python 3.10 or newer. No third-party packages are required.
- The source project's API key and secret key, plus the destination project's API key. The projects must have different API keys.
- Both projects must use the same data residency region: `us` or `eu`.
- Enough free disk space for a temporary ZIP export. The tool deletes each archive after processing it.

## Before you start

1. Clone the repository and enter its directory:

   ```bash
   git clone https://github.com/algusen/amplitude-migration-tool.git
   cd amplitude-migration-tool
   ```

2. Find the keys and data residency region in your Amplitude project settings.
3. For a first run, create a separate test destination project and choose a small period, such as one day. Historical ingestion cannot be undone with a single command.
4. Choose a stable name for each source, such as `web` and `mobile`. The name becomes part of each event's `insert_id` and must stay the same when you resume a migration.

## Run a migration

```bash
python3 migration.py \
  --from '2026-10-01T00:00:00+03:00' \
  --to '2026-10-02T00:00:00+03:00' \
  --source-name web \
  --region us
```

Replace the example dates with your period. Both timestamps must include a time zone (`Z` or an offset such as `+03:00`). `--from` is inclusive; `--to` is exclusive. The tool selects events by `event_time`, regardless of when Amplitude received them.

The command prompts for all three keys without displaying your input. For unattended runs, you can set environment variables instead:

```bash
export AMPLITUDE_SOURCE_API_KEY='source project API key'
export AMPLITUDE_SOURCE_SECRET_KEY='source project secret key'
export AMPLITUDE_DESTINATION_API_KEY='destination project API key'
python3 migration.py --from '2026-10-01T00:00:00Z' --to '2026-10-02T00:00:00Z' --source-name web --region us
```

The CLI also accepts `--source-api-key`, `--source-secret-key`, and `--destination-api-key`. Passing keys on the command line can expose them in shell history and process listings. Do not put real keys in the README, scripts, commits, or logs.

To migrate another source project, run the command again with that project's keys and a different `--source-name`, such as `mobile`. Use the same destination project key.

## Options

| Option | Description |
| --- | --- |
| `--from`, `--to` | Required period boundaries in ISO 8601 format with a time zone. |
| `--source-name` | Required stable source name, used in `insert_id`. |
| `--region` | Region of both projects: `us` (default) or `eu`. |
| `--late-arrival-hours` | Extra hours to scan for events uploaded late; default: `48`. |
| `--window-hours` | Hours per Export API request: `1`–`24`; default: `24`. |
| `--batch-size` | Events per Batch API request: `1`–`2000`; default: `100`. |
| `--checkpoint` | Path to a progress file; by default, one is created in `.migration-state/`. |

Run `python3 migration.py --help` for the complete CLI reference.

## Period boundaries and data completeness

The Export API selects archives by `server_upload_time`, while this tool selects events by `event_time`. To find late uploads, the tool scans from the start of the requested period through its end plus `--late-arrival-hours`, then keeps only events whose `event_time` falls within the requested period. The default 48-hour allowance is a tool setting, not an Amplitude guarantee. Events uploaded after that window will be missed. Choose an appropriate allowance before the first run.

Export data becomes available about two hours after Amplitude receives it. The tool checks that the entire calculated export window is available and stops before uploading anything if it is too recent. For a complete day with a 48-hour late-arrival allowance, wait for that allowance and the export delay before running the migration. Events uploaded to the source after the migration finishes are not added automatically.

## Resume and checkpoints

After each successfully processed window, the tool records completed hours and the number of submitted events in `.migration-state/`. The checkpoint contains neither keys nor event payloads. Rerun the same command with the same projects, source name, period, region, and late-arrival allowance to skip completed hours. Checkpoint files are excluded from Git.

If a run stops while uploading a window, some of its events may already be in the destination. The tool generates stable `insert_id` values, and Amplitude deduplicates repeated IDs within seven days. Resume an interrupted run within that period. After seven days, inspect the destination first: resending events may create duplicates.

If an export request exceeds the size limit, rerun with `--window-hours 1`. Completed hours remain in the checkpoint. If a single hour exceeds the Export API's 6 GB limit, use another export method, such as S3 Export.

## Verify the result

1. Confirm that the command exits with status `0` and reports completion.
2. Compare event counts and event types for the same period in the source and destination projects. Look up several known `user_id` or `device_id` values in User Lookup.
3. Check `platform`, event properties, and key funnel steps. If sources share event names, verify the expected filters and user identity behavior.

An `events_ingested` response confirms that the API accepted a batch. It does not by itself prove that reports or unique-event counts match.

## Migrated data and limitations

When present, the tool sends the event name and time, `user_id`, `device_id`, `platform`, event and user properties, session ID, and selected standard fields. It does not carry over the source `amplitude_id`; the destination project assigns its own identity. User property synchronization is disabled (`$skip_user_properties_sync`), so historical event properties do not overwrite the current user profile.

Tracking plans, charts, dashboards, cohorts, transformations, permissions, and Session Replay are outside this tool's scope. Review user identity rules and destination project settings separately. Migration across `us` and `eu` regions is not supported.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `401` or `403` | Source credentials, access, and project region. |
| `400` during export | Archive size; try `--window-hours 1`. |
| `429` or `5xx` | The tool retries with delays. If a longer rate limit is exhausted, rerun later. |
| `CERTIFICATE_VERIFY_FAILED` on macOS | System Python may not find root certificates. Try `SSL_CERT_FILE=/etc/ssl/cert.pem python3 migration.py ...`. Keep TLS verification enabled. |
| Checkpoint belongs to another run | Use the matching checkpoint or provide a new path with `--checkpoint`. |

## Development

```bash
python3 -m unittest discover -s tests -v
python3 migration.py --help
```

Amplitude references: [Export API](https://amplitude.com/docs/apis/analytics/export), [Batch Event Upload API](https://amplitude.com/docs/apis/analytics/batch-event-upload), [Data backfill](https://amplitude.com/docs/data/data-backfill).
