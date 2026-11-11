#!/usr/bin/env python3
"""Upload customer cluster snapshots or job failures through an OCI bucket PAR."""

import argparse
from datetime import datetime, timezone
from http.client import HTTPException
import json
from pathlib import Path
import re
import sys
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
    directory = "view" if kind == "cluster_view" else "job_data"
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
                        help="config JSON path (default: ./config.json)")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("-cluster_view", "--cluster_view", metavar="JSON_FILE")
    modes.add_argument("-job_data", "--job_data", metavar="JSON_FILE")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate locally and print the object name without uploading")
    args = parser.parse_args(argv)
    kind = "cluster_view" if args.cluster_view else "job_data"
    try:
        config = read_json(args.config)
        data = read_json(getattr(args, kind))
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
