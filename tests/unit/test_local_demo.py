"""Resolved demo configuration and destructive-command guards; never start Docker."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts/local_demo"


class LocalDemoConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("docker"):
            raise unittest.SkipTest("Docker Compose CLI required for config resolution")
        args = ["docker", "compose", "--env-file", ".env.example", "-p", "tlcn-local", "-f", "compose.yaml"]
        cls.config = json.loads(subprocess.check_output(args + ["config", "--format", "json"], cwd=ROOT))

    def test_published_ports_have_ordinary_bridge(self):
        c = self.config
        self.assertEqual(c["name"], "tlcn-local")
        self.assertFalse(c["networks"]["default"].get("internal", False))
        self.assertEqual(c["networks"]["default"]["driver"], "bridge")
        for service, port in [("airflow-webserver", 8085), ("minio", 9001), ("trino", 18080)]:
            bindings = c["services"][service]["ports"]
            self.assertEqual(len(bindings), 1)
            self.assertEqual((bindings[0]["host_ip"], int(bindings[0]["published"])), ("127.0.0.1", port))
        for name, service in c["services"].items():
            self.assertNotIn("network_mode", service)
            self.assertNotIn("container_name", service)
            if name not in ("airflow-webserver", "minio", "trino"):
                self.assertFalse(service.get("ports"))

    def test_local_routing_and_persistence(self):
        c = self.config
        self.assertEqual(len(c["services"]), 8)
        urls = []
        for name in ("runner", "airflow-scheduler", "airflow-webserver", "airflow-init"):
            service = c["services"][name]
            env = service["environment"]
            self.assertEqual(env["SILVER_LOCAL_PROFILE"], "local_nine")
            self.assertEqual(env["SILVER_LOCAL_ENABLED"], "1")
            self.assertEqual(env["SILVER_PRODUCTION_ENABLED"], "0")
            self.assertEqual(env["SILVER_CANARY_ONLY"], "0")
            self.assertFalse(env["SILVER_ACCEPTED_RELEASE"])
            self.assertFalse(env["SILVER_BRONZE_SCHEDULE"])
            mounts = {m["target"]: m for m in service["volumes"]}
            self.assertTrue(mounts["/workspace"]["read_only"])
            self.assertTrue(mounts["/workspace/data/raw"]["read_only"])
            if name.startswith("airflow-"):
                urls.append(env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
                self.assertEqual(env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"], "True")
                self.assertFalse(env["_PIP_ADDITIONAL_REQUIREMENTS"])
        self.assertEqual(len(set(urls)), 1)
        for name in ("postgres", "minio", "iceberg-rest"):
            self.assertFalse(c["services"][name].get("tmpfs"))
            self.assertTrue(any(m["type"] == "volume" for m in c["services"][name]["volumes"]))
        self.assertTrue(all(v["name"].startswith("tlcn-local_") and not v.get("external") for v in c["volumes"].values()))

    def test_memory_and_startup_commands(self):
        s = self.config["services"]
        for name, mib in [("runner", 3072), ("airflow-webserver", 1024), ("airflow-scheduler", 1536), ("trino", 1536)]:
            self.assertEqual(int(s[name]["mem_limit"]), mib * 1024 * 1024)
        self.assertEqual(s["airflow-webserver"]["environment"]["AIRFLOW__WEBSERVER__WORKERS"], "1")
        self.assertTrue(s["airflow-scheduler"]["healthcheck"]["test"])
        self.assertEqual(s["runner"]["command"], ["/workspace/jobs/silver/spark_api_server.py"])
        self.assertNotIn("pip install", " ".join(s["runner"]["command"]))
        self.assertEqual(s["airflow-init"]["image"], s["airflow-webserver"]["image"])
        self.assertIn("--console-address", " ".join(s["minio"]["command"]))


@unittest.skipUnless(shutil.which("powershell"), "Windows PowerShell required")
class LocalDemoSafetyTests(unittest.TestCase):
    def run_ps(self, text):
        with tempfile.TemporaryDirectory() as folder:
            script = Path(folder) / "test.ps1"
            script.write_text("$ErrorActionPreference='Stop'\n" + text, encoding="utf-8-sig")
            result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("GUARD_PASS", result.stdout)

    def body(self, name):
        # Keep the script's actual guard/control flow; replace only its imported
        # Docker plumbing with mocks so no writer, signal or deletion can run.
        return (SCRIPTS / name).read_text().replace(". (Join-Path $PSScriptRoot '_common.ps1')", "")

    def test_powershell_parse_all_entrypoints(self):
        self.run_ps("""
$bad=@()
Get-ChildItem scripts/local_demo/*.ps1 | ForEach-Object {
 $t=$null;$e=$null
 [System.Management.Automation.Language.Parser]::ParseFile($_.FullName,[ref]$t,[ref]$e) | Out-Null
 $bad+=@($e)
}
if ($bad.Count) { throw ($bad | Out-String) }
Write-Output 'GUARD_PASS'
""")

    def test_windows_json_root_array_is_enumerated_before_filtering(self):
        self.run_ps("""
. ./scripts/local_demo/_common.ps1
function docker {
 $global:LASTEXITCODE=0
 if ($args[0] -eq 'ps') { 'demo-id'; 'outside-id'; return }
 if ($args[0] -eq 'inspect') {
  '[{"Id":"demo-id","Config":{"Labels":{"com.docker.compose.project":"tlcn-local"}}},{"Id":"outside-id","Config":{"Labels":{}}}]'
  return
 }
 throw 'UNEXPECTED_DOCKER_MUTATION'
}
$all=@(Get-AllDemoContainers)
if ($all.Count -ne 2 -or $all[0].Id -ne 'demo-id') { throw 'JSON root array was not enumerated' }
$demo=@(Get-DemoContainers)
if ($demo.Count -ne 1 -or $demo[0].Id -ne 'demo-id') { throw 'Project filter failed' }
'GUARD_PASS'
""")

    def test_stop_refuses_active_writer_before_signal(self):
        self.run_ps("""
function Get-DemoContainers { [pscustomobject]@{State=[pscustomobject]@{Running=$true}} }
function Assert-DemoIdle { throw 'ACTIVE_WRITER' }
function Invoke-DemoDocker { throw 'UNEXPECTED_MUTATION' }
try { & {
""" + self.body("stop.ps1") + """
}; throw 'GUARD_MISSING' } catch { if ($_.Exception.Message -ne 'ACTIVE_WRITER') { throw }; 'GUARD_PASS' }
""")

    def test_idle_probe_requests_pid_and_rejects_live_jvm(self):
        self.run_ps("""
. ./scripts/local_demo/_common.ps1
function Invoke-DemoPython { }
$script:withJava=$false
function Invoke-DemoDocker {
 param([string[]]$Arguments,[switch]$Capture)
 if ($Arguments[0] -ne 'top' -or $Arguments[-1] -ne 'pid,stat,comm') { throw 'INVALID_TOP_OR_MUTATION' }
 if ($script:withJava) { return "PID STAT COMMAND`n123 Sl java" }
 return "PID STAT COMMAND`n123 Ss python3"
}
$containers=@(
 [pscustomobject]@{Id='scheduler';Config=[pscustomobject]@{Labels=[pscustomobject]@{'com.docker.compose.service'='airflow-scheduler'}};State=[pscustomobject]@{Running=$true}},
 [pscustomobject]@{Id='runner';Config=[pscustomobject]@{Labels=[pscustomobject]@{'com.docker.compose.service'='runner'}};State=[pscustomobject]@{Running=$true}}
)
Assert-DemoIdle $containers
$script:withJava=$true
try { Assert-DemoIdle $containers; throw 'GUARD_MISSING' } catch {
 if ($_.Exception.Message -ne 'Spark JVM still exists; reconcile it first.') { throw }
}
'GUARD_PASS'
""")

    def test_reset_default_is_plan_only(self):
        self.run_ps("""
$script:DemoProject='tlcn-local'
function Get-DemoConfig { @{} }
function Test-DemoRaw { }
function Get-DemoContainers { }
function docker { $global:LASTEXITCODE=0; if (($args -join ' ') -notmatch '^volume ls ') { throw 'UNEXPECTED_MUTATION' }; 'tlcn-local_local_minio_data' }
function Assert-DemoIdle { throw 'UNEXPECTED_IDLE_OR_WRITE' }
function Read-Host { throw 'UNEXPECTED_CONFIRMATION' }
& {
""" + self.body("reset.ps1") + """
}
'GUARD_PASS'
""")

    def test_reset_requires_exact_separate_confirmation(self):
        self.run_ps("""
$script:DemoProject='tlcn-local'
function Get-DemoConfig { @{} }
function Test-DemoRaw { }
function Get-DemoContainers { }
function docker { $global:LASTEXITCODE=0; if (($args -join ' ') -notmatch '^volume ls ') { throw 'UNEXPECTED_MUTATION' } }
function Read-Host { 'CANCEL' }
function Assert-DemoIdle { throw 'UNEXPECTED_MUTATION' }
try { & {
""" + self.body("reset.ps1") + """
} -DeleteData; throw 'GUARD_MISSING' } catch { if ($_.Exception.Message -ne 'Reset cancelled.') { throw }; 'GUARD_PASS' }
""")

    def test_start_checkonly_never_builds_or_starts(self):
        body = self.body("start.ps1")
        self.run_ps("""
$script:DemoRoot=(Get-Location).Path
function Get-DemoConfig { @{} }
function Test-DemoRaw { }
function Get-DemoMemory { [pscustomobject]@{WindowsAvailableGiB=3;WslAvailableGiB=5} }
function Invoke-DemoDocker { throw 'UNEXPECTED_MUTATION' }
function Assert-NoCompetingStacks { throw 'UNEXPECTED_MUTATION' }
& {
""" + body + """
} -CheckOnly
'GUARD_PASS'
""")
