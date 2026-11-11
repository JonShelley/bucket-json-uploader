#!/usr/bin/env python3
"""Upload cluster views, host maps, or job failures through an OCI bucket PAR."""

import argparse
import csv
from datetime import datetime, timezone
from http.client import HTTPException
import json
from pathlib import Path
import re
import sys
import subprocess
import shutil
import tempfile
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def reject_constant(value):
    raise ValueError("JSON must not contain NaN or Infinity")


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("JSON must not contain duplicate keys")
        result[key] = value
    return result


def read_json(path):
    try:
        with Path(path).open(encoding="utf-8") as source:
            value = json.load(source, parse_constant=reject_constant,
                              object_pairs_hook=unique_keys)
    except (OSError, UnicodeError, ValueError):
        raise ValueError(f"Cannot read {path}: expected a readable, valid UTF-8 JSON file") from None
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def nonempty_string(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")


def count(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def timestamp(value, name):
    nonempty_string(value, name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{name} must be an ISO 8601 timestamp with a timezone") from None
    if "T" not in value or parsed.tzinfo is None:
        raise ValueError(f"{name} must be an ISO 8601 timestamp with a timezone")
    return parsed.astimezone(timezone.utc)


def string_list(value, name):
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list of strings")
    for item in value:
        nonempty_string(item, name)
    if len(set(value)) != len(value):
        raise ValueError(f"{name} must not contain duplicates")


def validate_config(config):
    for key in ("customer_id", "cluster_id"):
        value = config.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
            raise ValueError(f"{key} must be 1–128 letters, digits, dots, underscores or dashes, starting with a letter or digit")
    url = config.get("par_url")
    if not isinstance(url, str):
        raise ValueError("par_url must be a full HTTPS bucket PAR URL ending in /o/")
    try:
        parts = urlsplit(url)
        valid = (parts.scheme == "https" and parts.hostname and not parts.username
                 and not parts.password and not parts.query and not parts.fragment
                 and re.fullmatch(r"/p/[^/]+/n/[^/]+/b/[^/]+/o/", parts.path)
                 and not any(c.isspace() or ord(c) < 32 for c in url))
        parts.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("par_url must be a full HTTPS bucket PAR URL ending in /o/ (no object name, query or fragment)")


def validate_data(data, kind):
    if kind == "cluster_map":
        validate_cluster_map(data)
        return
    if kind == "job_data":
        if "time" in data:
            timestamp(data["time"], "time")
        string_list(data.get("host_serials"), "host_serials")
        if not data["host_serials"]:
            raise ValueError("host_serials must contain at least one host serial")
        nonempty_string(data.get("reason"), "reason")
        return
    if "observed_at" in data:
        timestamp(data["observed_at"], "observed_at")
    for key in ("total_nodes", "offline_nodes"):
        count(data.get(key), key)
    if data["offline_nodes"] > data["total_nodes"]:
        raise ValueError("offline_nodes cannot exceed total_nodes")
    string_list(data.get("healthy_nodes"), "healthy_nodes")
    unhealthy = data.get("unhealthy_nodes")
    if not isinstance(unhealthy, dict):
        raise ValueError("unhealthy_nodes must be an object mapping host serials to reasons")
    for serial, reason in unhealthy.items():
        nonempty_string(serial, "unhealthy_nodes host serial")
        nonempty_string(reason, "unhealthy_nodes reason")
    if set(data["healthy_nodes"]) & set(unhealthy):
        raise ValueError("healthy_nodes and unhealthy_nodes must not overlap")
    if len(data["healthy_nodes"]) + len(unhealthy) > data["total_nodes"]:
        raise ValueError("node lists cannot contain more nodes than total_nodes")
    summary_key = "racks_with_16_plus_healthy_cts"
    if summary_key in data:
        count(data[summary_key], summary_key)
    if "racks" not in data:
        return
    racks = data["racks"]
    if not isinstance(racks, dict):
        raise ValueError("racks must be an object keyed by rack ID")
    for rack_id, rack in racks.items():
        nonempty_string(rack_id, "rack ID")
        if not isinstance(rack, dict):
            raise ValueError("each rack must be an object")
        for key in ("total_nodes", "healthy"):
            count(rack.get(key), f"rack {rack_id} {key}")
        serials = rack.get("unhealthy")
        if not isinstance(serials, str):
            raise ValueError("rack unhealthy must be a comma-separated string of host serials (empty if none)")
        hosts = [serial.strip() for serial in serials.split(",")] if serials else []
        string_list(hosts, f"rack {rack_id} unhealthy serials")
        if rack["healthy"] + len(hosts) > rack["total_nodes"]:
            raise ValueError(f"rack {rack_id} healthy and unhealthy counts cannot exceed total_nodes")
    if sum(rack["total_nodes"] for rack in racks.values()) > data["total_nodes"]:
        raise ValueError("rack totals cannot exceed cluster total_nodes")
    qualifying = sum(rack["healthy"] >= 16 for rack in racks.values())
    if summary_key in data and data[summary_key] != qualifying:
        raise ValueError("racks_with_16_plus_healthy_cts does not match the rack details")
    data[summary_key] = qualifying


def read_slurm_nodes():
    """Query and validate the inventory from the locally configured controller."""
    try:
        result = subprocess.run(["scontrol", "--json", "show", "nodes"],
                                capture_output=True, text=True, check=True, timeout=30)
    except FileNotFoundError:
        raise ValueError("scontrol was not found; run discovery on a Slurm client or load its Slurm module") from None
    except subprocess.TimeoutExpired:
        raise ValueError("Slurm discovery timed out after 30 seconds; check controller connectivity") from None
    except (subprocess.CalledProcessError, OSError, UnicodeError):
        raise ValueError("Slurm discovery failed; verify that 'scontrol --json show nodes' works for this user") from None
    try:
        response = json.loads(result.stdout, parse_constant=reject_constant,
                              object_pairs_hook=unique_keys)
    except ValueError:
        raise ValueError("scontrol returned invalid JSON; Slurm JSON output support is required") from None
    if not isinstance(response, dict) or response.get("errors"):
        raise ValueError("Slurm returned an error response; run 'scontrol --json show nodes' to diagnose")
    nodes = response.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("Slurm returned no node inventory; refusing to create an empty cluster view")
    names = []
    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError("Slurm returned an invalid node record")
        names.append(node.get("name"))
    string_list(names, "Slurm node names")
    return nodes


def discover_slurm(node_map=None):
    """Build a cluster view from the controller reachable by the local scontrol."""
    nodes = read_slurm_nodes()
    if node_map is not None and not isinstance(node_map, dict):
        raise ValueError("node map must be an object keyed by Slurm NodeName")
    data = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "source": "slurm",
        "node_identifier": "host_serial" if node_map is not None else "slurm_node_name",
        "total_nodes": len(nodes), "offline_nodes": 0,
        "healthy_nodes": [], "unhealthy_nodes": {},
    }
    healthy_states = {"IDLE", "ALLOCATED", "MIXED", "COMPLETING"}
    healthy_flags = {"RESERVED", "PLANNED", "CLOUD", "DYNAMIC_NORM"}
    offline_states = {"DOWN", "UNKNOWN", "FUTURE", "NOT_RESPONDING", "POWERED_DOWN",
                      "POWERING_DOWN", "POWERING_UP", "REBOOT_ISSUED"}
    names, identifiers, racks = set(), set(), {}
    rack_nodes = 0
    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError("Slurm returned an invalid node record")
        name = node.get("name")
        nonempty_string(name, "Slurm node name")
        if name in names:
            raise ValueError("Slurm returned duplicate node names")
        names.add(name)
        states = node.get("state")
        if isinstance(states, str):
            states = states.split("+")
        string_list(states, "Slurm node state")
        if not states:
            raise ValueError("Slurm returned a node without a state")
        flags = {state.strip().upper() for state in states}
        reason = node.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ValueError("Slurm returned an invalid node reason")
        identifier, rack_id = name, None
        if node_map is not None:
            entry = node_map.get(name)
            if not isinstance(entry, dict):
                raise ValueError(f"node map is missing an entry for {name}")
            identifier = entry.get("host_serial")
            nonempty_string(identifier, f"host_serial for {name}")
            if "rack_id" in entry:
                rack_id = entry["rack_id"]
                nonempty_string(rack_id, f"rack_id for {name}")
        if identifier in identifiers or "," in identifier or identifier != identifier.strip():
            raise ValueError("node identifiers must be unique, without commas or surrounding whitespace")
        identifiers.add(identifier)
        healthy = bool(flags & healthy_states) and flags <= healthy_states | healthy_flags
        data["offline_nodes"] += bool(flags & offline_states)
        if healthy:
            data["healthy_nodes"].append(identifier)
        else:
            state_reason = "Slurm state: " + "+".join(sorted(flags))
            data["unhealthy_nodes"][identifier] = (f"{reason.strip()} ({state_reason})"
                                                   if reason and reason.strip() else state_reason)
        if rack_id is not None:
            rack_nodes += 1
            rack = racks.setdefault(rack_id, {"total_nodes": 0, "healthy": 0, "unhealthy": ""})
            rack["total_nodes"] += 1
            rack["healthy"] += healthy
            if not healthy:
                rack["unhealthy"] += ("," if rack["unhealthy"] else "") + identifier
    if racks:
        if rack_nodes != len(nodes):
            raise ValueError("provide rack_id for every discovered node or omit rack_id from all entries")
        data["racks"] = racks
    validate_data(data, "cluster_view")
    return data


def validate_cluster_map(data):
    hosts = data.get("host_info")
    if not isinstance(hosts, dict) or not hosts:
        raise ValueError("host_info must be a nonempty object keyed by host name")
    serials, ocids = set(), set()
    for name, info in hosts.items():
        nonempty_string(name, "host name")
        if not isinstance(info, dict):
            raise ValueError("each host_info entry must contain serial and ocid")
        for key in ("serial", "ocid"):
            nonempty_string(info.get(key), key)
            if any(c.isspace() for c in info[key]) or "," in info[key]:
                raise ValueError(f"{key} must not contain whitespace or commas")
        if info["serial"] in serials or info["ocid"] in ocids:
            raise ValueError("host serials and OCIDs must be unique within a cluster map")
        serials.add(info["serial"])
        ocids.add(info["ocid"])
        if "rack_id" in info:
            nonempty_string(info["rack_id"], "rack_id")


def read_cluster_map(path):
    if Path(path).suffix.lower() == ".csv":
        data = {"host_info": {}}
        try:
            with Path(path).open(encoding="utf-8-sig", newline="") as source:
                rows = csv.reader(source, strict=True, skipinitialspace=True)
                header = next(rows, [])
                if [value.strip().lower().replace(" ", "_") for value in header] != ["host_name", "host_serial", "host_ocid"]:
                    raise ValueError("CSV header must be: host_name,host_serial,host_ocid")
                for row in rows:
                    if not row:
                        continue
                    if len(row) != 3:
                        raise ValueError("each CSV row must contain host name, host serial, and host OCID")
                    name, serial, ocid = (value.strip() for value in row)
                    if name in data["host_info"]:
                        raise ValueError("CSV contains a duplicate host name")
                    data["host_info"][name] = {"serial": serial, "ocid": ocid}
        except (OSError, UnicodeError, csv.Error):
            raise ValueError("cannot read cluster map CSV; expected readable UTF-8 CSV") from None
    else:
        data = read_json(path)
    validate_cluster_map(data)
    return data


def read_node_map(path):
    data = read_cluster_map(path) if Path(path).suffix.lower() == ".csv" else read_json(path)
    if "host_info" not in data:
        return data  # Original node maps with host_serial remain supported.
    validate_cluster_map(data)
    return {name: {"host_serial": info["serial"],
                   **({"rack_id": info["rack_id"]} if "rack_id" in info else {})}
            for name, info in data["host_info"].items()}


# The fixed command runs on each target, never on the collecting login node.
HOST_INFO_COMMAND = (
    "set -eu; "
    "serial=$(sudo -n dmidecode -s system-serial-number); "
    "ocid=$(curl -fsS --connect-timeout 5 --max-time 10 --noproxy 169.254.169.254 "
    "-H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/id); "
    "printf 'BUCKET_MAP_SERIAL=%s\\nBUCKET_MAP_OCID=%s\\n' \"$serial\" \"$ocid\""
)


def parse_host_info(output):
    lines = output.splitlines()
    if len(lines) != 2 or not lines[0].startswith("BUCKET_MAP_SERIAL=") or not lines[1].startswith("BUCKET_MAP_OCID="):
        raise ValueError("unexpected host command output")
    info = {"serial": lines[0].split("=", 1)[1].strip(), "ocid": lines[1].split("=", 1)[1].strip()}
    if not info["ocid"].startswith("ocid1.instance.") or not info["ocid"].split(".")[-1]:
        raise ValueError("IMDS did not return an instance OCID")
    if info["serial"].lower() in {"none", "unknown", "not specified", "to be filled by o.e.m."}:
        raise ValueError("dmidecode did not return a usable serial")
    validate_cluster_map({"host_info": {"host": info}})
    return info


def discover_cluster_map():
    names = [node["name"] for node in read_slurm_nodes()]
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) for name in names):
        raise ValueError("Slurm node names must be individual SSH host names (letters, digits, dots, underscores, dashes)")
    hosts, failed = {}, []
    clush = shutil.which("clush")
    if clush:
        print(f"Collecting {len(names)} hosts with clush (fanout 32)", file=sys.stderr)
        with tempfile.TemporaryDirectory(prefix="bucket-map-") as directory:
            folder = Path(directory)
            hostfile = folder / "hosts"
            hostfile.write_text("\n".join(names) + "\n")
            outdir = folder / "stdout"
            command = [clush, "--nostdin", "-S", "-R", "ssh", "-f", "32", "-t", "10", "-u", "30",
                       "-o", "-oBatchMode=yes", f"--hostfile={hostfile}",
                       f"--outdir={outdir}", f"--errdir={folder / 'stderr'}", HOST_INFO_COMMAND]
            try:
                result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                        text=True, timeout=((len(names) + 31) // 32) * 45 + 30)
            except subprocess.TimeoutExpired:
                raise ValueError("clush collection timed out; no cluster map was saved") from None
            except (OSError, UnicodeError):
                raise ValueError("could not execute clush; check the ClusterShell installation") from None
            for name in names:
                try:
                    hosts[name] = parse_host_info((outdir / name).read_text())
                except (OSError, UnicodeError, ValueError):
                    failed.append(name)
            if result.returncode and not failed:
                raise ValueError("clush reported remote command failure; no cluster map was saved")
    else:
        ssh = shutil.which("ssh")
        if not ssh:
            raise ValueError("cluster-map discovery requires clush or ssh")
        print("clush unavailable; collecting hosts sequentially with SSH", file=sys.stderr)
        for name in names:
            try:
                result = subprocess.run([ssh, "-n", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                                         name, HOST_INFO_COMMAND], capture_output=True, text=True, check=True, timeout=40)
                hosts[name] = parse_host_info(result.stdout)
            except (OSError, UnicodeError, subprocess.SubprocessError, ValueError):
                failed.append(name)
    if failed:
        raise ValueError("could not collect serial/OCID for: " + ", ".join(failed) +
                         "; check SSH, noninteractive sudo for dmidecode, and IMDS access; no map saved")
    data = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "host_info": hosts}
    validate_cluster_map(data)
    return data


def write_cluster_map(path, data):
    if Path(path).suffix.lower() not in {".json", ".csv"}:
        raise ValueError("cluster map output path must end in .json or .csv")
    validate_cluster_map(data)
    with Path(path).open("x", encoding="utf-8", newline="") as output:
        if Path(path).suffix.lower() == ".csv":
            writer = csv.writer(output)
            writer.writerow(["host_name", "host_serial", "host_ocid"])
            writer.writerows((name, info["serial"], info["ocid"]) for name, info in data["host_info"].items())
        else:
            output.write(json.dumps(data, ensure_ascii=False, allow_nan=False, indent=2) + "\n")


def prepare_upload(config, data, kind):
    validate_config(config)
    validate_data(data, kind)
    now = datetime.now(timezone.utc)
    # Accept the original event-time fields as well as the common timestamp field.
    time_field = "observed_at" if kind == "cluster_view" else "time"
    if "timestamp" in data:
        event_time = timestamp(data["timestamp"], "timestamp")
    elif time_field in data:
        event_time = timestamp(data[time_field], time_field)
    else:
        event_time = now
    data["timestamp"] = event_time.isoformat(timespec="microseconds").replace("+00:00", "Z")
    directory = "view" if kind == "cluster_view" else kind
    filename = event_time.strftime("%y%m%d:%H:%M:%S" if kind == "cluster_view"
                                   else "%y%m%d:%H:%M:%S.%f")
    object_name = f"{config['customer_id']}/{config['cluster_id']}/{directory}/{filename}.json"
    document = {
        "schema_version": 2,
        "customer_id": config["customer_id"],
        "cluster_id": config["cluster_id"],
        "data_type": kind,
        "submitted_at": now.isoformat().replace("+00:00", "Z"),
        "data": data,
    }
    body = json.dumps(document, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return object_name, body


def upload(par_url, object_name, body):
    request = Request(par_url + quote(object_name, safe="/"), data=body,
                      headers={"Content-Type": "application/json", "If-None-Match": "*"}, method="PUT")
    try:
        with build_opener(NoRedirects()).open(request, timeout=60) as response:
            if not 200 <= response.status < 300:
                raise ValueError(f"Upload failed (HTTP {response.status})")
    except HTTPError as error:
        status = error.code
        error.close()
        if status == 412:
            raise ValueError("Upload refused (HTTP 412): an object with this timestamp already exists; no overwrite performed") from None
        raise ValueError(f"Upload failed (HTTP {status}); check PAR permissions, expiry and prefix scope") from None
    except (URLError, OSError, ValueError, HTTPException):
        # Network exception text can include the secret PAR URL.
        raise ValueError("Upload failed; check network connectivity, TLS and the PAR configuration") from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("-config", "--config", default="config.json",
                        help="upload config JSON path (default: ./config.json)")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("-cluster_view", "--cluster_view", metavar="JSON_FILE")
    modes.add_argument("-job_data", "--job_data", metavar="JSON_FILE")
    modes.add_argument("-cluster_map", "--cluster_map", metavar="JSON_OR_CSV",
                       help="upload a host name/serial/OCID map")
    modes.add_argument("-discover_cluster_map", "--discover-cluster-map", metavar="JSON_OR_CSV",
                       help="collect a Slurm cluster host map with clush (or SSH) and save locally")
    modes.add_argument("-discover_slurm", "--discover-slurm", metavar="JSON_FILE",
                       help="query Slurm and create a local cluster-view JSON file without uploading")
    parser.add_argument("--node-map", metavar="JSON_OR_CSV",
                        help="cluster map (JSON/CSV) or original node-map JSON for Slurm discovery")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate locally and print the object name without uploading")
    args = parser.parse_args(argv)
    if args.node_map and not args.discover_slurm:
        parser.error("--node-map requires --discover-slurm")
    if (args.discover_slurm or args.discover_cluster_map) and args.dry_run:
        parser.error("--dry-run applies to uploads; discovery only creates a local file")
    if args.discover_cluster_map:
        try:
            if Path(args.discover_cluster_map).suffix.lower() not in {".json", ".csv"}:
                raise ValueError("cluster map output path must end in .json or .csv")
            if Path(args.discover_cluster_map).exists():
                raise FileExistsError()
            data = discover_cluster_map()
            write_cluster_map(args.discover_cluster_map, data)
            print(f"Created: {args.discover_cluster_map} ({len(data['host_info'])} hosts)")
            return 0
        except FileExistsError:
            print("Error: output file already exists; choose a new path", file=sys.stderr)
        except OSError:
            print("Error: cannot write the cluster-map file", file=sys.stderr)
        except ValueError as error:
            print(f"Error: {error}", file=sys.stderr)
        return 1
    if args.discover_slurm:
        try:
            node_map = read_node_map(args.node_map) if args.node_map else None
            data = discover_slurm(node_map)
            body = json.dumps(data, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
            with Path(args.discover_slurm).open("x", encoding="utf-8") as output:
                output.write(body)
            print(f"Created: {args.discover_slurm} ({data['total_nodes']} Slurm nodes; identifiers: {data['node_identifier']})")
            return 0
        except FileExistsError:
            print("Error: output file already exists; choose a new path", file=sys.stderr)
        except OSError:
            print("Error: cannot write the cluster-view JSON file", file=sys.stderr)
        except ValueError as error:
            print(f"Error: {error}", file=sys.stderr)
        return 1
    kind = "cluster_view" if args.cluster_view else ("cluster_map" if args.cluster_map else "job_data")
    try:
        config = read_json(args.config)
        data = read_cluster_map(args.cluster_map) if kind == "cluster_map" else read_json(getattr(args, kind))
        object_name, body = prepare_upload(config, data, kind)
        if not args.dry_run:
            upload(config["par_url"], object_name, body)
        print(f"{'Validated (no upload)' if args.dry_run else 'Uploaded'}: {object_name}")
        return 0
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
