import copy
import importlib.util
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "check_architecture", Path(__file__).with_name("check-architecture.py")
)
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class ArchitecturePolicyTests(unittest.TestCase):
    def setUp(self):
        self.packages = [
            {"name": name, "dependencies": [
                {"name": target, "kind": None} for target in sorted(targets)
            ]}
            for name, targets in policy.ALLOWED.items()
        ]

    def package(self, name):
        return next(package for package in self.packages if package["name"] == name)

    def test_approved_graph_passes(self):
        self.assertEqual(policy.violations(self.packages), [])

    def test_optional_transport_dependency_on_server_is_rejected(self):
        self.package("json-rpc-2-tcp")["dependencies"].append(
            {"name": "json-rpc-2-server", "kind": None, "optional": True}
        )
        self.assertTrue(any("Forbidden" in error for error in policy.violations(self.packages)))

    def test_dev_dependency_does_not_change_production_graph(self):
        self.package("json-rpc-2-server")["dependencies"].append(
            {"name": "json-rpc-2-runtime", "kind": "dev"}
        )
        self.assertEqual(policy.violations(self.packages), [])

    def test_network_runtime_in_core_is_rejected(self):
        self.package("json-rpc-2-core")["dependencies"].append(
            {"name": "tokio", "kind": None}
        )
        self.assertTrue(any("Non-wire" in error for error in policy.violations(self.packages)))

    def test_build_dependency_cannot_bypass_boundary(self):
        self.package("json-rpc-2-core")["dependencies"].append(
            {"name": "json-rpc-2-runtime", "kind": "build"}
        )
        self.assertTrue(any("Forbidden" in error for error in policy.violations(self.packages)))

    def test_unknown_workspace_crate_requires_explicit_policy(self):
        self.packages.append({"name": "unreviewed-crate", "dependencies": []})
        self.assertTrue(any("No architecture policy" in error for error in policy.violations(self.packages)))

    def test_production_cycle_is_reported(self):
        self.package("json-rpc-2-core")["dependencies"].append(
            {"name": "json-rpc-2-transport", "kind": None}
        )
        self.assertTrue(any("cycle" in error for error in policy.violations(self.packages)))

    def test_missing_crate_is_rejected(self):
        packages = copy.deepcopy(self.packages)
        packages = [package for package in packages if package["name"] != "json-rpc-2-runtime"]
        self.assertTrue(any("missing" in error for error in policy.violations(packages)))


if __name__ == "__main__":
    unittest.main()
