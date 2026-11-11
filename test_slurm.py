from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import upload


def node(name, state, reason=None):
    return {"name": name, "state": state, "reason": reason}


class SlurmTests(unittest.TestCase):
    def discover(self, nodes, mapping=None):
        result = subprocess.CompletedProcess([], 0, json.dumps({"nodes": nodes, "errors": []}), "")
        with patch.object(upload.subprocess, "run", return_value=result) as query:
            data = upload.discover_slurm(mapping)
        query.assert_called_once_with(["scontrol", "--json", "show", "nodes"],
                                      capture_output=True, text=True, check=True, timeout=30)
        return data

    def test_scheduler_states_and_failure_reasons(self):
        nodes = [
            node("idle", ["IDLE"]), node("busy", ["ALLOCATED"]),
            node("mixed", ["MIXED"]), node("completing", ["ALLOCATED", "COMPLETING"]),
            node("reserved", ["IDLE", "RESERVED"]), node("cloud", ["IDLE", "CLOUD"]),
            node("draining", ["ALLOCATED", "DRAIN"], "GPU health check failed"),
            node("down", ["DOWN", "NOT_RESPONDING"], "Not responding"),
            node("noresp", ["IDLE", "NOT_RESPONDING"]),
            node("power", ["IDLE", "POWERED_DOWN"]), node("future", ["FUTURE"]),
            node("unknown", ["UNKNOWN"]), node("maint", ["IDLE", "MAINTENANCE"]),
            node("newflag", ["IDLE", "UNRECOGNIZED_FLAG"]),
            node("fail", ["IDLE", "FAIL"]), node("reboot", ["IDLE", "REBOOT_ISSUED"]),
        ]
        data = self.discover(nodes)
        self.assertEqual(data["total_nodes"], 16)
        self.assertEqual(data["offline_nodes"], 6)
        self.assertEqual(data["healthy_nodes"], ["idle", "busy", "mixed", "completing", "reserved", "cloud"])
        self.assertEqual(len(data["unhealthy_nodes"]), 10)
        self.assertIn("GPU health check failed", data["unhealthy_nodes"]["draining"])
        self.assertIn("UNRECOGNIZED_FLAG", data["unhealthy_nodes"]["newflag"])
        self.assertEqual(data["node_identifier"], "slurm_node_name")
        self.assertNotIn("racks", data)
        self.assertNotIn("racks_with_16_plus_healthy_cts", data)
        self.assertTrue(data["timestamp"].endswith("Z"))
        self.assertEqual(data["source"], "slurm")

    def test_serial_mapping_and_rack_aggregation(self):
        nodes = [node(f"n{i}", ["IDLE"] if i < 16 else ["DOWN"], "Offline") for i in range(18)]
        mapping = {f"n{i}": {"host_serial": f"HOST{i}", "rack_id": "rack-01"} for i in range(18)}
        data = self.discover(nodes, mapping)
        self.assertEqual(data["node_identifier"], "host_serial")
        self.assertEqual(data["racks"], {"rack-01": {"total_nodes": 18, "healthy": 16, "unhealthy": "HOST16,HOST17"}})
        self.assertEqual(data["racks_with_16_plus_healthy_cts"], 1)
        self.assertEqual(data["healthy_nodes"], [f"HOST{i}" for i in range(16)])
        self.assertEqual(set(data["unhealthy_nodes"]), {"HOST16", "HOST17"})

    def test_mapping_without_racks_and_string_state(self):
        data = self.discover([node("n1", "IDLE"), node("n2", "ALLOCATED+DRAIN", "Maintenance")],
                             {"n1": {"host_serial": "A"}, "n2": {"host_serial": "B"}})
        self.assertEqual(data["healthy_nodes"], ["A"])
        self.assertIn("B", data["unhealthy_nodes"])
        self.assertEqual(data["offline_nodes"], 0)
        self.assertNotIn("racks", data)

    def test_incomplete_and_colliding_mappings_fail(self):
        nodes = [node("n1", ["IDLE"]), node("n2", ["DOWN"])]
        for mapping in (
            {}, {"n1": {"host_serial": "A"}},
            {"n1": {"host_serial": "A"}, "n2": {"host_serial": "A"}},
            {"n1": {"host_serial": "A", "rack_id": "rack1"}, "n2": {"host_serial": "B"}},
            {"n1": {"host_serial": "A"}, "n2": {"host_serial": ""}},
            {"n1": {"host_serial": "A,B"}, "n2": {"host_serial": "C"}},
            {"n1": {"host_serial": " A"}, "n2": {"host_serial": "B"}},
        ):
            with self.subTest(mapping=mapping), self.assertRaises(ValueError):
                self.discover(nodes, mapping)

    def test_bad_records_fail_instead_of_reporting_partial_inventory(self):
        for nodes in ([], [None], [node("", ["IDLE"])], [node("n", [])],
                      [node("n", None)], [node("n", [123])], [node("n", ["IDLE"], 7)],
                      [node("n", ["IDLE"]), node("n", ["DOWN"])]):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                self.discover(nodes)

    def test_command_and_json_failures(self):
        for error in (FileNotFoundError(), subprocess.TimeoutExpired("scontrol", 30),
                      subprocess.CalledProcessError(1, "scontrol")):
            with self.subTest(error=type(error).__name__), patch.object(upload.subprocess, "run", side_effect=error), self.assertRaises(ValueError):
                upload.discover_slurm()
        for body in ('invalid', '[]', '{}', '{"nodes": {}}',
                     '{"errors": [{"error": "denied"}], "nodes": []}'):
            result = subprocess.CompletedProcess([], 0, body, "")
            with self.subTest(body=body), patch.object(upload.subprocess, "run", return_value=result), self.assertRaises(ValueError):
                upload.discover_slurm()

    def test_discovery_does_not_upload_or_read_config_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "view.json"
            data = self.discover([node("n1", ["IDLE"])])
            with patch.object(upload, "discover_slurm", return_value=data), \
                    patch.object(upload, "upload") as send, patch.object(upload, "read_json") as read, \
                    redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                self.assertEqual(upload.main(["--discover-slurm", str(target)]), 0)
                original = target.read_bytes()
                self.assertEqual(upload.main(["--discover-slurm", str(target)]), 1)
                self.assertEqual(target.read_bytes(), original)
            send.assert_not_called()
            read.assert_not_called()
            self.assertEqual(json.loads(original)["total_nodes"], 1)

    def test_failed_query_does_not_create_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "view.json"
            with patch.object(upload, "discover_slurm", side_effect=ValueError("unreachable")), redirect_stderr(StringIO()):
                self.assertEqual(upload.main(["--discover-slurm", str(target)]), 1)
            self.assertFalse(target.exists())

    def test_cli_end_to_end_with_fake_slurm_client(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            fake = folder / "scontrol"
            payload = {"nodes": [node("n1", ["ALLOCATED"]), node("n2", ["DOWN"], "Link failure")]}
            fake.write_text(f"#!{sys.executable}\nimport sys\nassert sys.argv[1:] == ['--json', 'show', 'nodes']\nprint({json.dumps(payload)!r})\n")
            fake.chmod(0o700)
            output = folder / "view.json"
            mapping = folder / "map.json"
            mapping.write_text(json.dumps({"n1": {"host_serial": "S1"}, "n2": {"host_serial": "S2"}}))
            script = Path(upload.__file__).resolve()
            env = dict(os.environ, PATH=str(folder) + os.pathsep + os.environ.get("PATH", ""))
            result = subprocess.run([sys.executable, str(script), "-discover_slurm", str(output), "--node-map", str(mapping)],
                                    cwd=folder, env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            data = json.loads(output.read_text())
            self.assertEqual(data["healthy_nodes"], ["S1"])
            self.assertIn("Link failure", data["unhealthy_nodes"]["S2"])
            result = subprocess.run([sys.executable, str(script), "--config", str(script.parent / "config.example.json"),
                                     "-cluster_view", str(output), "--dry-run"],
                                    cwd=folder, env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("/view/", result.stdout)

    def test_discovery_options_cannot_be_mixed_with_upload_modes(self):
        for arguments in (["--discover-slurm", "out.json", "-job_data", "job.json"],
                          ["-cluster_view", "view.json", "--node-map", "map.json"],
                          ["--discover-slurm", "out.json", "--dry-run"]):
            with self.subTest(arguments=arguments), redirect_stderr(StringIO()), self.assertRaises(SystemExit) as caught:
                upload.main(arguments)
            self.assertEqual(caught.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
