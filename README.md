# bucket-json-uploader

MVP bucket uploader. Currently supports OCI Object Storage buckets only.

Upload customer cluster snapshots, host identity maps, and job failure events to OCI Object
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

`-config` is also accepted. Upload modes are `-cluster_view FILE`, `-job_data FILE`,
and `-cluster_map FILE`; choose exactly one. Each upload sends one JSON object.
For multiple job failures, run one command per event. Double-dash spellings such
as `--cluster_map` also work.

## Upload or collect a cluster map

A cluster map links host names to hardware serials and OCI instance OCIDs.
Upload it as either UTF-8 JSON or CSV:

```bash
python3 upload.py --config config.json -cluster_map cluster-map.json
python3 upload.py --config config.json -cluster_map cluster-map.csv
```

Both formats are uploaded as JSON under
`<customer>/<cluster>/cluster_map/YYMMDD:HH:MM:SS.ffffff.json`, with the usual
customer/cluster envelope and `data_type: "cluster_map"`. JSON accepts an optional
UTC `timestamp`; if absent, the uploader adds one. CSV has exactly three columns,
so its upload timestamp is generated when uploading.

JSON format (`examples/cluster_map.json`):

```json
{
  "host_info": {
    "node001": {"serial": "HOST001", "ocid": "ocid1.instance.oc1.example.unique001"},
    "node002": {"serial": "HOST002", "ocid": "ocid1.instance.oc1.example.unique002"}
  }
}
```

CSV format (`examples/cluster_map.csv`), including the required header:

```csv
host_name,host_serial,host_ocid
node001,HOST001,ocid1.instance.oc1.example.unique001
node002,HOST002,ocid1.instance.oc1.example.unique002
```

The header also accepts spaces, such as `host name,host serial,host ocid`, and
ignores capitalization. Every entry must provide all three values. Duplicate
host names, serials, or OCIDs are rejected. JSON may additionally include a
`rack_id` inside each host entry for use with Slurm view discovery.

To collect the map automatically from the Slurm cluster:

```bash
# Create JSON (includes the UTC collection timestamp)
python3 upload.py --discover-cluster-map cluster-map.json

# Or create the same map as three-column CSV
python3 upload.py --discover-cluster-map cluster-map.csv

# Use the collected map when generating a cluster view
python3 upload.py --discover-slurm cluster-view.json --node-map cluster-map.json
```

`-discover_cluster_map` is an alias. Collection requires no bucket config and
does not upload automatically. The script obtains node names from
`scontrol --json show nodes`, then runs these queries on every node:

```bash
sudo -n dmidecode -s system-serial-number
curl -fsS --connect-timeout 5 --max-time 10 --noproxy 169.254.169.254 \
  -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/id
```

The IMDS value is the **instance OCID**, not a physical host OCID. Serial and
OCID results are tied to the Slurm NodeName used to address that node. NodeNames
must therefore be resolvable SSH destinations from the collecting machine.
Rack IDs are not discovered by these two commands and can be added to JSON later.

When `clush` is on PATH, collection uses it with fanout 32, a host file, separate
per-host output files, a 10-second connection timeout, a 30-second remote command
timeout, and remote exit-code checking. This requires ClusterShell support for
`--hostfile`, `--outdir`, and `--errdir`. Without `clush`, the script reports that
it is falling back to sequential SSH, with a 40-second timeout per host. Both
paths use noninteractive SSH; existing SSH configuration and host-key checks apply.

Each node needs `curl`, `dmidecode`, IMDS access, and sudo rights that allow
`sudo -n dmidecode -s system-serial-number` without a password prompt. The `-n`
option ensures a missing permission fails rather than hanging collection.
Collection includes all nodes returned by Slurm, including down/FUTURE nodes;
unreachable hosts will fail collection. Failed or malformed responses are
reported by host name, and no partial map is saved. Existing output files are
never overwritten. Correct access/connectivity and rerun with a fresh path.

See [ClusterShell output collection](https://clustershell.readthedocs.io/en/stable/tools/clush.html#saving-output-in-files)
and [OCI instance metadata](https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/gettingmetadata.htm).

## Discover a cluster from Slurm

Run on a login or compute node where `scontrol --json show nodes` can reach the
cluster controller. The Slurm client must support JSON output and have its data
parser plugins installed. No extra Python packages or root access are needed.

```bash
# Generate a local cluster view; no PAR or config is needed
python3 upload.py --discover-slurm cluster-view.json

# Generate using hardware serials and optional rack IDs
python3 upload.py --discover-slurm cluster-view.json --node-map examples/slurm_node_map.json

# Upload the generated file with your customer/cluster config
python3 upload.py --config config.json -cluster_view cluster-view.json
```

Use a new output path for each discovery run: an existing file is never
overwritten. `-discover_slurm` is also accepted. Discovery and upload are separate
commands, so the generated file can be reviewed or enriched before it is sent.
`--dry-run` applies to uploads only. Failure to query or parse Slurm returns exit
code 1 and does not create a snapshot. A query has a 30-second timeout.

Discovery queries the controller once and uses all returned node records,
including configured FUTURE nodes. Visibility depends on the Slurm permissions
of the invoking user. It does not restrict the view to the current job allocation.
Make sure the Slurm client targets the cluster named in the upload config.

The snapshot includes the current UTC `timestamp`, node counts, healthy nodes,
and unhealthy nodes with the Slurm reason and state. `source` is `slurm`.
Classification is a scheduler-health snapshot, not a hardware or RDMA health
check:

| Classification | Policy |
| --- | --- |
| Healthy | IDLE, ALLOCATED, MIXED, or COMPLETING, optionally with RESERVED, PLANNED, CLOUD, or DYNAMIC_NORM flags |
| Unhealthy | Every other state/flag combination, including DRAIN, FAIL, maintenance, reboot pending, and unrecognized flags |
| Offline | Any DOWN, UNKNOWN, FUTURE, NOT_RESPONDING, POWERED_DOWN, POWERING_DOWN, POWERING_UP, or REBOOT_ISSUED flag |

Offline nodes are a subset of unhealthy nodes. A drained node may still be
running jobs and is not automatically counted as offline. Powered-down capacity
may be intentional; the reason/state remains available for interpretation.

Slurm NodeName is not necessarily a hardware serial number. Without a node map,
the snapshot uses NodeName in both node fields, sets `node_identifier` to
`slurm_node_name`, and omits `racks`. This is the discovery-specific exception to
the host-serial convention used for manually prepared input.

To use actual serials, pass a JSON/CSV cluster map with `--node-map`. The
original node-map format below also remains supported, with entries keyed by
each exact Slurm NodeName:

```json
{
  "node001": {"host_serial": "HOST001", "rack_id": "rack-01"},
  "node002": {"host_serial": "HOST002", "rack_id": "rack-01"}
}
```

The map must cover every discovered node with a unique `host_serial`. When used,
`node_identifier` becomes `host_serial`. Include `rack_id` for all nodes to
generate rack totals, healthy counts, comma-separated unhealthy serials, and
`racks_with_16_plus_healthy_cts`. Omit `rack_id` from every entry for clusters
without rack grouping. Partial serial or rack mappings are rejected so the
snapshot cannot silently mix identities or undercount racks. Additional map
entries for nodes not returned by Slurm are ignored. Rack IDs are supplied by
the customer; network switches and hostname prefixes are not assumed to be racks.

See the official [scontrol JSON output documentation](https://slurm.schedmd.com/scontrol.html#OPT_json)
and [Slurm node states](https://slurm.schedmd.com/sinfo.html#SECTION_NODE-STATE-CODES).

## Input JSON

See the complete files in `examples/`.

All upload modes accept an optional `timestamp` field in the customer JSON, such as
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

Additional JSON fields are preserved inside `data`. CSV cluster maps are normalized to `host_info` first. Inputs must be UTF-8 JSON
objects; duplicate keys, NaN, and Infinity are rejected. JSON files are read into
memory, making this suitable for small snapshots and events.

## Stored objects

Object Storage directories are object-name prefixes:

```text
customer-a/cluster-01/
  view/261111:15:00:00.json
  job_data/261111:14:52:00.123456.json
  cluster_map/261111:15:00:00.123456.json
```

The hierarchy is customer → cluster → `view`, `job_data`, or `cluster_map`, with no extra date
directories. View filenames use `YYMMDD:HH:MM:SS.json`; job and map filenames use
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
are mocked; a live OCI upload requires an actual PAR. Slurm tests use synthetic
controller responses; run discovery on your cluster to verify its client setup.
Cluster-map tests cover CSV/JSON conversion, clush per-host output, SSH fallback,
remote command execution with synthetic sudo/curl programs, and failure handling.
They do not contact a live cluster or IMDS endpoint.
