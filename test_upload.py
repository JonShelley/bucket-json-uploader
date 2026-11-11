import copy
from datetime import datetime, timezone
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import upload


ROOT = Path(__file__).parent


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.config = upload.read_json(ROOT / "config.example.json")
        self.cluster = upload.read_json(ROOT / "examples/cluster_view.json")
        self.job = upload.read_json(ROOT / "examples/job_data.json")

    def test_cluster_envelope_and_routing(self):
        name, body = upload.prepare_upload(self.config, self.cluster, "cluster_view")
        document = json.loads(body)
        self.assertEqual(name, "customer-a/cluster-01/view/261111:15:00:00.json")
        self.assertEqual(document["data"]["racks_with_16_plus_healthy_cts"], 1)
        self.assertEqual(document["customer_id"], "customer-a")
        self.assertEqual(document["schema_version"], 2)
        self.assertNotIn("par_url", document)
        self.assertEqual(document["data"]["timestamp"], "2026-11-11T15:00:00.000000Z")

    def test_job_envelope_and_routing(self):
        name, body = upload.prepare_upload(self.config, self.job, "job_data")
        self.assertIn("/job_data/", name)
        self.assertEqual(json.loads(body)["data"], self.job)

    def test_customer_timestamp_is_normalized_and_used_for_name(self):
        self.job["timestamp"] = "2026-11-11T08:52:00.123456-06:00"
        name, body = upload.prepare_upload(self.config, self.job, "job_data")
        self.assertEqual(name, "customer-a/cluster-01/job_data/261111:14:52:00.123456.json")
        self.assertEqual(json.loads(body)["data"]["timestamp"], "2026-11-11T14:52:00.123456Z")
        self.job["timestamp"] = "2026-11-11T14:52:00.123457Z"
        self.assertNotEqual(name, upload.prepare_upload(self.config, self.job, "job_data")[0])

    def test_missing_timestamp_is_added_without_changing_source_file(self):
        for kind, data, old_field, directory, filename in (
            ("cluster_view", self.cluster, "timestamp", "view", "261111:17:01:02"),
            ("job_data", self.job, "timestamp", "job_data", "261111:17:01:02.345678"),
        ):
            data.pop(old_field)
            with patch.object(upload, "datetime") as clock:
                clock.now.return_value = datetime(2026, 11, 11, 17, 1, 2, 345678, timezone.utc)
                name, body = upload.prepare_upload(self.config, data, kind)
            self.assertEqual(name, f"customer-a/cluster-01/{directory}/{filename}.json")
            self.assertEqual(json.loads(body)["data"]["timestamp"], "2026-11-11T17:01:02.345678Z")

    def test_legacy_event_time_is_used_when_timestamp_is_missing(self):
        for kind, data, key in (("cluster_view", self.cluster, "observed_at"),
                                ("job_data", self.job, "time")):
            value = data.pop("timestamp")
            data[key] = value
            _, body = upload.prepare_upload(self.config, data, kind)
            self.assertEqual(json.loads(body)["data"]["timestamp"],
                             upload.timestamp(value, key).isoformat(timespec="microseconds").replace("+00:00", "Z"))

    def test_invalid_customer_timestamp_is_rejected(self):
        for value in (None, "", "yesterday", "2026-11-11T12:00:00"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                upload.prepare_upload(self.config, {**self.job, "timestamp": value}, "job_data")

    def test_invalid_cluster_data(self):
        cases = [
            {"offline_nodes": 19}, {"total_nodes": True},
            {"healthy_nodes": ["HOST017"]}, {"unhealthy_nodes": ["HOST017"]},
            {"unhealthy_nodes": {"HOST017": ""}},
            {"unhealthy_nodes": {"": "Offline"}},
            {"unhealthy_nodes": {"HOST017": None}},
            {"unhealthy_nodes": {"HOST017": {"reason": "Offline"}}},
            {"racks_with_16_plus_healthy_cts": 0},
            {"observed_at": "2026-11-11T15:00:00"},
            {"total_nodes": 1, "racks": {}},
        ]
        for change in cases:
            with self.subTest(change=change), self.assertRaises(ValueError):
                upload.validate_data({**copy.deepcopy(self.cluster), **change}, "cluster_view")

    def test_racks_are_optional(self):
        data = upload.read_json(ROOT / "examples/cluster_view_no_racks.json")
        _, body = upload.prepare_upload(self.config, data, "cluster_view")
        stored = json.loads(body)["data"]
        self.assertNotIn("racks", stored)
        self.assertNotIn("racks_with_16_plus_healthy_cts", stored)
        self.assertEqual(stored["unhealthy_nodes"], {"HOST002": "Offline; host unreachable"})

    def test_empty_racks_and_unhealthy_nodes_are_valid(self):
        self.cluster.update(racks={}, unhealthy_nodes={})
        upload.validate_data(self.cluster, "cluster_view")
        self.assertEqual(self.cluster["racks_with_16_plus_healthy_cts"], 0)

    def test_rack_details_are_preserved_and_summary_counts_threshold(self):
        racks = {
            "rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": "HOST017, HOST018"},
            "rack-02": {"total_nodes": 16, "healthy": 15, "unhealthy": "HOST034"},
            "rack-03": {"total_nodes": 17, "healthy": 17, "unhealthy": ""},
        }
        self.cluster.update(total_nodes=51, racks=copy.deepcopy(racks))
        _, body = upload.prepare_upload(self.config, self.cluster, "cluster_view")
        stored = json.loads(body)["data"]
        self.assertEqual(stored["racks"], racks)
        self.assertEqual(stored["racks_with_16_plus_healthy_cts"], 2)
        self.assertEqual(stored["unhealthy_nodes"]["HOST018"], "RDMA link failure")

    def test_invalid_rack_details(self):
        for racks in (
            [], None, {"": {}}, {"rack-01": []},
            {"rack-01": {"total_nodes": 18, "healthy": 16}},
            {"rack-01": {"total_nodes": 18, "healthy": 19, "unhealthy": ""}},
            {"rack-01": {"total_nodes": -1, "healthy": 0, "unhealthy": ""}},
            {"rack-01": {"total_nodes": 18, "healthy": True, "unhealthy": ""}},
            {"rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": ["A"]}},
            {"rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": "A,A"}},
            {"rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": "A,"}},
            {"rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": " "}},
            {"rack-01": {"total_nodes": 18, "healthy": 17, "unhealthy": "A,B"}},
            {"rack-01": {"total_nodes": 19, "healthy": 16, "unhealthy": ""}},
        ):
            with self.subTest(racks=racks), self.assertRaises(ValueError):
                upload.validate_data({**copy.deepcopy(self.cluster), "racks": racks}, "cluster_view")

    def test_summary_without_racks_is_validated_but_not_inferred(self):
        self.cluster.pop("racks")
        self.cluster["racks_with_16_plus_healthy_cts"] = 1
        upload.validate_data(self.cluster, "cluster_view")
        self.assertEqual(self.cluster["racks_with_16_plus_healthy_cts"], 1)
        self.cluster["racks_with_16_plus_healthy_cts"] = -1
        with self.assertRaises(ValueError):
            upload.validate_data(self.cluster, "cluster_view")

    def test_invalid_job_data(self):
        for change in ({"time": "yesterday"}, {"host_serials": []}, {"reason": ""}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                upload.validate_data({**self.job, **change}, "job_data")

    def test_config_rejects_unsafe_ids_and_non_bucket_urls(self):
        for change in (
            {"customer_id": "../escape"}, {"cluster_id": "x/y"},
            {"par_url": self.config["par_url"].replace("https:", "http:")},
            {"par_url": self.config["par_url"] + "object.json"},
            {"par_url": self.config["par_url"] + "?secret=1"},
            {"par_url": "https://[invalid"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                upload.validate_config({**self.config, **change})

    def test_put_request(self):
        opener = MagicMock()
        opener.open.return_value.__enter__.return_value.status = 200
        with patch.object(upload, "build_opener", return_value=opener):
            upload.upload(self.config["par_url"], "customer/cluster/job_data/test.json", b'{"ok":true}')
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), "PUT")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(request.get_header("If-none-match"), "*")
        self.assertEqual(request.data, b'{"ok":true}')
        self.assertTrue(request.full_url.endswith("/o/customer/cluster/job_data/test.json"))
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 60)

    def test_timestamp_collision_is_reported_without_overwrite(self):
        with patch.object(upload, "build_opener") as factory:
            factory.return_value.open.side_effect = HTTPError("secret", 412, "exists", {}, BytesIO())
            with self.assertRaisesRegex(ValueError, "timestamp already exists"):
                upload.upload(self.config["par_url"], "test.json", b"{}")

    def test_network_and_http_errors_do_not_expose_par(self):
        secret = self.config["par_url"]
        for error in (URLError(secret), HTTPError(secret, 403, secret, {}, BytesIO(b"denied"))):
            with patch.object(upload, "build_opener") as factory:
                factory.return_value.open.side_effect = error
                with self.assertRaises(ValueError) as caught:
                    upload.upload(secret, "test.json", b"{}")
                self.assertNotIn(secret, str(caught.exception))
                self.assertNotIn("REPLACE_WITH_PAR_TOKEN", str(caught.exception))

    def test_redirects_are_refused(self):
        self.assertIsNone(upload.NoRedirects().redirect_request(None, None, 307, "", {}, "https://other.example"))

    def test_strict_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            for source in ('{"x":NaN}', '{"x":1,"x":2}', '[]', '{broken'):
                path.write_text(source)
                with self.subTest(source=source), self.assertRaises(ValueError):
                    upload.read_json(path)

    def test_cli_dry_run_does_not_upload(self):
        output = StringIO()
        with patch.object(upload, "upload") as send, redirect_stdout(output):
            status = upload.main(["--config", str(ROOT / "config.example.json"),
                                  "-cluster_view", str(ROOT / "examples/cluster_view.json"), "--dry-run"])
        self.assertEqual(status, 0)
        send.assert_not_called()
        self.assertIn("Validated (no upload)", output.getvalue())

    def test_default_config_and_upload(self):
        with patch.object(upload, "read_json", side_effect=[self.config, self.job]) as read, \
                patch.object(upload, "upload") as send, redirect_stdout(StringIO()):
            self.assertEqual(upload.main(["-job_data", "failure.json"]), 0)
        self.assertEqual(read.call_args_list[0].args, ("config.json",))
        send.assert_called_once()

    def test_invalid_data_never_uploads(self):
        with patch.object(upload, "read_json", side_effect=[self.config, {}]), \
                patch.object(upload, "upload") as send, redirect_stderr(StringIO()):
            self.assertEqual(upload.main(["-job_data", "invalid.json"]), 1)
        send.assert_not_called()

    def test_modes_are_required_and_exclusive(self):
        for arguments in ([], ["-cluster_view", "a.json", "-job_data", "b.json"]):
            with self.subTest(arguments=arguments), redirect_stderr(StringIO()), self.assertRaises(SystemExit) as caught:
                upload.main(arguments)
            self.assertEqual(caught.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
