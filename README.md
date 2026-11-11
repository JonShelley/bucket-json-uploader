# bucket-json-uploader

MVP bucket uploader. Currently supports OCI Object Storage buckets only.

Upload customer cluster snapshots and individual job failure events to OCI Object
Storage through a write-only bucket pre-authenticated request (PAR). Requires
Python 3.9 or later; no packages, OCI SDK, CLI, or customer API keys are needed.

## Customer setup

From this directory:

```bash
cp config.example.json config.json
chmod 600 config.json
```

Edit `config.json` with the customer ID, cluster ID, and full PAR URL supplied by
the bucket administrator. The URL must end in `/o/`; the script appends an object
name. Customer and cluster IDs use letters, digits, dots, underscores or dashes,
start with a letter or digit, and have at most 128 characters.

```bash
# Default config: config.json in the current working directory
python3 upload.py -cluster_view examples/cluster_view.json
python3 upload.py -job_data examples/job_data.json

# Select a different customer/cluster config
python3 upload.py --config customer-a.json -cluster_view snapshot.json
python3 upload.py --config customer-a.json -job_data failure.json

# Validate without sending anything (works with the example placeholder URL)
python3 upload.py --config config.example.json -cluster_view examples/cluster_view.json --dry-run
```

`-config` is also accepted. Exactly one of `-cluster_view FILE` or `-job_data FILE`
is required. Each command sends one JSON object. For multiple job failures, run
one command per event. `--cluster_view` and `--job_data` also work.

## Input JSON

See the complete files in `examples/`.

Both modes accept an optional `timestamp` field in the customer JSON, such as
`"timestamp": "2026-11-11T14:52:00.123456Z"`. It must be an ISO 8601 timestamp
with a timezone. The uploader normalizes it to UTC and uses it for the object
filename. Supply microseconds for job failures that occur within the same second.

If `timestamp` is absent, the uploader uses the original `observed_at` (cluster)
or `time` (job) field if provided; otherwise it uses the current UTC time.
It adds `timestamp` to the uploaded `data` in UTC with six fractional digits and
a `Z` suffix. The customer's local JSON file is not modified. An invalid supplied
timestamp is rejected rather than replaced.

Cluster view fields:

| Field | Value |
| --- | --- |
| `timestamp` | Optional snapshot time, ISO 8601 with timezone; defaults to current UTC time |
| `observed_at` | Optional original snapshot-time field, used if `timestamp` is absent |
| `total_nodes` | Nonnegative integer, total cluster inventory |
| `offline_nodes` | Nonnegative integer count, no greater than total nodes |
| `racks` | Optional object keyed by rack ID; each value contains `total_nodes`, `healthy`, and `unhealthy` |
| `healthy_nodes` | Array of unique healthy host serial strings |
| `unhealthy_nodes` | Object mapping each unhealthy host serial to a nonempty reason string; `{}` if none |
| `racks_with_16_plus_healthy_cts` | Calculated from rack `healthy` counts when `racks` is present; if supplied alongside racks, it must match. Without racks it is optional and not inferred. |

Example rack details and unhealthy host reasons:

```json
{
  "racks": {
    "rack-01": {
      "total_nodes": 18,
      "healthy": 16,
      "unhealthy": "HOST017,HOST018"
    }
  },
  "unhealthy_nodes": {
    "HOST017": "Offline; host unreachable",
    "HOST018": "RDMA link failure"
  }
}
```

The example above shows only the changed fields; see `examples/cluster_view.json`
for a complete payload. For clusters where rack grouping does not apply, omit
`racks` entirely, as in `examples/cluster_view_no_racks.json`. An empty rack
object is also accepted and produces a qualifying rack count of zero. Omitted
racks do not cause the uploader to add a rack summary.

Rack `total_nodes` and `healthy` are nonnegative integers. Rack `unhealthy` is a
comma-separated string of unique host serials, or `""` when none are unhealthy;
spaces around serials are allowed. Healthy count plus listed unhealthy hosts
cannot exceed the rack total. Combined rack totals cannot exceed the cluster
total. Rack entries and node lists may be partial, so exact equality is not
required and rack serials are not required to appear in the cluster mapping.

Healthy host serials and keys in `unhealthy_nodes` must not overlap or together
exceed `total_nodes`. Offline remains an independently supplied count. The
customer supplies health classifications and reasons. Rack `healthy` is used
for the existing `racks_with_16_plus_healthy_cts` summary; the uploader does not
discover health or perform a separate CT-to-node conversion.

This payload format uses envelope `schema_version: 2`. Previous array-based
`racks` and `unhealthy_nodes` inputs must be updated to the objects shown above.
Existing objects in the bucket are unchanged.

Job failure fields:

| Field | Value |
| --- | --- |
| `timestamp` | Optional failure time, ISO 8601 with timezone; include microseconds when available |
| `time` | Optional original failure-time field, used if `timestamp` is absent |
| `host_serials` | Nonempty array of unique host serial strings |
| `reason` | Nonempty string describing the failure |
| `job_id` | Optional job identifier |

Additional JSON fields are preserved inside `data`. Inputs must be UTF-8 JSON
objects; duplicate keys, NaN, and Infinity are rejected. JSON files are read into
memory, making this suitable for small snapshots and events.

## Stored objects

Object Storage directories are object-name prefixes:

```text
customer-a/cluster-01/
  view/261111:15:00:00.json
  job_data/261111:14:52:00.123456.json
```

The hierarchy is customer → cluster → `view` or `job_data`, with no extra date
directories. View filenames use `YYMMDD:HH:MM:SS.json`; job filenames use
`YYMMDD:HH:MM:SS.ffffff.json`, where `ffffff` is the six-digit microsecond
fraction of the second. Both use the selected timestamp in UTC. The command
option and envelope `data_type` remain `cluster_view` for views.

The uploaded document wraps the input with identifying metadata:

```json
{
  "schema_version": 2,
  "customer_id": "customer-a",
  "cluster_id": "cluster-01",
  "data_type": "job_data",
  "submitted_at": "2026-11-11T15:00:00Z",
  "data": {
    "timestamp": "2026-11-11T14:52:00.123456Z",
    "job_id": "job-12345",
    "host_serials": ["HOST003", "HOST004"],
    "reason": "Job failed after an RDMA transport timeout"
  }
}
```

The PAR URL is excluded from the document and command output. Upload uses HTTPS
PUT with a 60-second timeout and refuses redirects. Success prints the object
name. Exit codes: `0` success (or successful dry run), `1` validation/upload
failure, `2` incorrect command usage. HTTP errors show a status without the PAR.

Uploads use `If-None-Match: *` to prevent replacing an existing object. Two views
for the same cluster within the same UTC second have the same name; the second
upload fails with HTTP 412. Job timestamps with identical microseconds also
collide. Microseconds reduce collisions but are not a unique job identifier.
If a customer supplies job timestamps with only whole seconds, the script cannot
recover missing precision and writes `.000000` in the filename.

There are no automatic retries or local queue. A timeout can occur after OCI
accepted a write. Retrying with the same timestamp fails with HTTP 412 if that
object already exists; retrying without a supplied time uses a new timestamp and
can duplicate an event. Use an optional `job_id` to help downstream consumers
identify related events.

## Bucket administrator setup

Create the bucket and a PAR in OCI before distributing a customer config:

1. In the bucket's **Pre-authenticated requests**, create a request.
2. Choose **Bucket**, or **Object with prefix** scoped to
   `customer-a/cluster-01/` (recommended for a shared bucket).
3. Choose **Permit object writes** (or writes to the specified prefix). Leave
   object listing disabled and set an expiration suitable for the MVP.
4. Copy the full HTTPS URL into the customer's config as `par_url`.

A specific-object PAR cannot support this uploader's multiple object names.
A prefix-scoped PAR still uses the bucket URL ending in `/o/`; the generated
object name must match its configured scope.

The script cannot inspect PAR permissions or expiry through a write-only URL;
write-only access must be configured in OCI. Anyone possessing a PAR can write
within its scope, including replacing existing objects. Customer/cluster config
IDs are labels, not authenticated identity or enforced isolation. A customer can
edit them, so enforce customer boundaries with prefix-scoped PARs or separate
buckets. This uploader's conditional PUT prevents accidental replacement; a PAR
holder using another client could still replace objects within the allowed scope.

Treat config files as secrets. Local default config filenames are ignored by
Git, but arbitrary config paths are not automatically protected. Revoke/replace
the PAR if exposed. This project does not create OCI resources or distribute PARs.

Reference: [Oracle PAR creation and PUT examples](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/usingpreauthenticatedrequests_topic-To_create_a_preauthenticated_request_for_all_objects_in_a_bucket.htm).

## Verification

```bash
python3 -m unittest -v
```

Tests cover routing and envelopes, UTC conversion and timestamp fallback,
microsecond names, collision errors, validation, command modes and defaults,
PUT construction, redirect refusal, and secret-free upload errors. Network calls
are mocked; a live OCI upload requires an actual PAR.
