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


ROOT = Path(upload.__file__).parent


def host_output(serial="SERIAL1", ocid="ocid1.instance.oc1.test.unique1"):
    return f"BUCKET_MAP_SERIAL={serial}\nBUCKET_MAP_OCID={ocid}\n"


class ClusterMapTests(unittest.TestCase):
    def setUp(self):
        self.data = upload.read_cluster_map(ROOT / "examples/cluster_map.json")
        self.nodes = [{"name": "node001", "state": ["IDLE"]}, {"name": "node002", "state": ["DOWN"]}]

    def test_json_csv_equivalence_and_upload_envelope(self):
        self.assertEqual(upload.read_cluster_map(ROOT / "examples/cluster_map.csv"), self.data)
        self.data["timestamp"] = "2026-11-11T10:02:03.123456-07:00"
        name, body = upload.prepare_upload(upload.read_json(ROOT / "config.example.json"), self.data, "cluster_map")
        self.assertEqual(name, "customer-a/cluster-01/cluster_map/261111:17:02:03.123456.json")
        stored = json.loads(body)
        self.assertEqual(stored["data_type"], "cluster_map")
        self.assertEqual(stored["data"]["host_info"], self.data["host_info"])
        self.assertEqual(stored["data"]["timestamp"], "2026-11-11T17:02:03.123456Z")

    def test_maps_feed_slurm_discovery_and_preserve_optional_racks(self):
        for path in (ROOT / "examples/cluster_map.json", ROOT / "examples/cluster_map.csv"):
            mapping = upload.read_node_map(path)
            with patch.object(upload, "read_slurm_nodes", return_value=self.nodes):
                view = upload.discover_slurm(mapping)
            self.assertEqual(view["healthy_nodes"], ["HOST001"])
            self.assertEqual(set(view["unhealthy_nodes"]), {"HOST002"})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "map.json"
            self.data["host_info"]["node001"]["rack_id"] = "rack-1"
            path.write_text(json.dumps(self.data))
            self.assertEqual(upload.read_node_map(path)["node001"]["rack_id"], "rack-1")

    def test_csv_headers_bom_quotes_and_whitespace(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "map.csv"
            path.write_text('\ufeffhost name,host serial,host ocid\r\n"node1", "S1", "ocid1.instance.x"\r\n')
            self.assertEqual(upload.read_cluster_map(path)["host_info"], {"node1": {"serial": "S1", "ocid": "ocid1.instance.x"}})

    def test_bad_csv_and_duplicate_hosts_are_rejected(self):
        header = "host_name,host_serial,host_ocid\n"
        for text in ("", "a,b,c\nx,y,z\n", header, header + "node1,S1\n", header + "node1,S1,O1,extra\n",
                     header + "node1,S1,O1\nnode1,S2,O2\n", header + "node1,,O1\n",
                     header + "node1,S1,O1\nnode2,S1,O2\n", header + '"unclosed'):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "map.csv"
                path.write_text(text)
                with self.assertRaises(ValueError):
                    upload.read_cluster_map(path)

    def test_json_validation(self):
        for data in ({}, {"host_info": []}, {"host_info": {}}, {"host_info": {"": {}}},
                     {"host_info": {"node": {"serial": "S"}}},
                     {"host_info": {"node": {"serial": "S", "ocid": None}}},
                     {"host_info": {"node": {"serial": "S 1", "ocid": "O"}}},
                     {"host_info": {"n1": {"serial": "S1", "ocid": "O"}, "n2": {"serial": "S2", "ocid": "O"}}}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                upload.validate_cluster_map(data)

    def test_output_round_trip_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            for suffix in ("json", "csv"):
                path = Path(directory) / ("map." + suffix)
                upload.write_cluster_map(path, self.data)
                self.assertEqual(upload.read_cluster_map(path), self.data)
                with self.assertRaises(FileExistsError):
                    upload.write_cluster_map(path, self.data)
                self.assertEqual(upload.read_cluster_map(path), self.data)

    def test_clush_uses_hostfile_and_per_host_output(self):
        def run(command, **kwargs):
            self.assertIn("-S", command)
            self.assertEqual(command[command.index("-f") + 1], "32")
            self.assertIn("-oBatchMode=yes", command)
            self.assertEqual(command[-1], upload.HOST_INFO_COMMAND)
            hostfile = Path(next(arg.split("=", 1)[1] for arg in command if arg.startswith("--hostfile=")))
            self.assertEqual(hostfile.read_text().splitlines(), ["node001", "node002"])
            outdir = Path(next(arg.split("=", 1)[1] for arg in command if arg.startswith("--outdir=")))
            outdir.mkdir()
            for i, name in enumerate(hostfile.read_text().splitlines()):
                (outdir / name).write_text(host_output(f"SERIAL{i}", f"ocid1.instance.oc1.test.unique{i}"))
            return subprocess.CompletedProcess(command, 0)
        with patch.object(upload, "read_slurm_nodes", return_value=self.nodes), \
                patch.object(upload.shutil, "which", return_value="/fake/clush"), \
                patch.object(upload.subprocess, "run", side_effect=run), redirect_stderr(StringIO()):
            data = upload.discover_cluster_map()
        self.assertEqual(data["host_info"]["node001"]["serial"], "SERIAL0")
        self.assertTrue(data["timestamp"].endswith("Z"))

    def test_clush_failures_do_not_create_partial_map(self):
        for outcome in (subprocess.CompletedProcess([], 1), subprocess.TimeoutExpired("clush", 30)):
            with self.subTest(outcome=type(outcome).__name__), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "map.json"
                with patch.object(upload, "read_slurm_nodes", return_value=self.nodes), \
                        patch.object(upload.shutil, "which", return_value="/fake/clush"), \
                        patch.object(upload.subprocess, "run", side_effect=outcome if isinstance(outcome, Exception) else None,
                                     return_value=outcome), redirect_stderr(StringIO()):
                    self.assertEqual(upload.main(["--discover-cluster-map", str(path)]), 1)
                self.assertFalse(path.exists())

    def test_ssh_fallback_and_failed_host_reporting(self):
        responses = [subprocess.CompletedProcess([], 0, host_output("S1", "ocid1.instance.x.one")),
                     subprocess.CompletedProcess([], 0, host_output("S2", "ocid1.instance.x.two"))]
        with patch.object(upload, "read_slurm_nodes", return_value=self.nodes), \
                patch.object(upload.shutil, "which", side_effect=lambda name: None if name == "clush" else "/fake/ssh"), \
                patch.object(upload.subprocess, "run", side_effect=responses) as run, redirect_stderr(StringIO()):
            self.assertEqual(len(upload.discover_cluster_map()["host_info"]), 2)
            self.assertEqual(run.call_count, 2)
            self.assertIn("BatchMode=yes", run.call_args.args[0])
        responses[1] = subprocess.CalledProcessError(1, "ssh")
        with patch.object(upload, "read_slurm_nodes", return_value=self.nodes), \
                patch.object(upload.shutil, "which", side_effect=lambda name: None if name == "clush" else "/fake/ssh"), \
                patch.object(upload.subprocess, "run", side_effect=responses), redirect_stderr(StringIO()), \
                self.assertRaisesRegex(ValueError, "node002"):
            upload.discover_cluster_map()

    def test_remote_command_runs_requested_queries_and_stops_on_sudo_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / "sudo").write_text('#!/bin/sh\n[ "$*" = "-n dmidecode -s system-serial-number" ] || exit 99\nprintf SERIAL123\n')
            (folder / "curl").write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$MAP_ARGS"\nprintf ocid1.instance.oc1.test.unique\n')
            for name in ("sudo", "curl"):
                (folder / name).chmod(0o700)
            capture = folder / "args"
            env = dict(os.environ, PATH=directory, MAP_ARGS=str(capture))
            result = subprocess.run(["/bin/sh", "-c", upload.HOST_INFO_COMMAND], env=env, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(upload.parse_host_info(result.stdout)["serial"], "SERIAL123")
            args = capture.read_text().splitlines()
            self.assertIn("Authorization: Bearer Oracle", args)
            self.assertIn("http://169.254.169.254/opc/v2/instance/id", args)
            capture.unlink()
            (folder / "sudo").write_text('#!/bin/sh\nexit 1\n')
            result = subprocess.run(["/bin/sh", "-c", upload.HOST_INFO_COMMAND], env=env, capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(capture.exists())

    def test_untrusted_output_and_invalid_targets_fail(self):
        for output in ("", host_output(""), host_output("Not Specified"), host_output(ocid="<html>error</html>"),
                       host_output() + "extra\n", host_output("A\nB")):
            with self.subTest(output=output), self.assertRaises(ValueError):
                upload.parse_host_info(output)
        for name in ("../escape", "-option", "node;touch /tmp/unwanted", "node[1-2]"):
            with patch.object(upload, "read_slurm_nodes", return_value=[{"name": name}]), \
                    patch.object(upload.subprocess, "run") as run, self.assertRaises(ValueError):
                upload.discover_cluster_map()
            run.assert_not_called()

    def test_upload_cli_normalizes_csv(self):
        with patch.object(upload, "upload") as send, redirect_stdout(StringIO()):
            self.assertEqual(upload.main(["--config", str(ROOT / "config.example.json"), "-cluster_map", str(ROOT / "examples/cluster_map.csv")]), 0)
        self.assertIn("/cluster_map/", send.call_args.args[1])
        self.assertEqual(json.loads(send.call_args.args[2])["data"]["host_info"], self.data["host_info"])

    def test_existing_output_stops_before_any_remote_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "map.json"
            target.write_text("existing")
            with patch.object(upload, "discover_cluster_map") as discover, redirect_stderr(StringIO()):
                self.assertEqual(upload.main(["--discover-cluster-map", str(target)]), 1)
            discover.assert_not_called()
            self.assertEqual(target.read_text(), "existing")


    def test_collection_cli_end_to_end(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            fake_slurm = folder / "scontrol"
            fake_slurm.write_text(f"#!{sys.executable}\nimport json\nprint(json.dumps({dict(nodes=self.nodes)!r}))\n")
            fake_clush = folder / "clush"
            fake_clush.write_text(f"#!{sys.executable}\n" + """import sys
from pathlib import Path
args = sys.argv[1:]
out = Path(next(a.split('=', 1)[1] for a in args if a.startswith('--outdir=')))
hosts = Path(next(a.split('=', 1)[1] for a in args if a.startswith('--hostfile='))).read_text().splitlines()
out.mkdir()
for i, name in enumerate(hosts):
    (out / name).write_text(f'BUCKET_MAP_SERIAL=S{i}\\nBUCKET_MAP_OCID=ocid1.instance.test.unique{i}\\n')
""")
            fake_slurm.chmod(0o700)
            fake_clush.chmod(0o700)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ.get("PATH", ""))
            for suffix in ("json", "csv"):
                output = folder / ("map." + suffix)
                result = subprocess.run([sys.executable, str(ROOT / "upload.py"), "--discover-cluster-map", str(output)],
                                        cwd=folder, env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(upload.read_cluster_map(output)["host_info"]["node001"]["serial"], "S0")
                view = folder / ("view-" + suffix + ".json")
                result = subprocess.run([sys.executable, str(ROOT / "upload.py"), "--discover-slurm", str(view), "--node-map", str(output)],
                                        cwd=folder, env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(view.read_text())["healthy_nodes"], ["S0"])


if __name__ == "__main__":
    unittest.main()
