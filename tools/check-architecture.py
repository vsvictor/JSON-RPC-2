#!/usr/bin/env python3
"""Check production crate boundaries using Cargo's actual manifest metadata."""

import json
import subprocess
import sys
from pathlib import Path


ALLOWED = {
    "json-rpc-2-core": set(),
    "json-rpc-2-transport": {"json-rpc-2-core"},
    "json-rpc-2-client": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-server": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-tcp": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-datagram": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-quic": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-iot": {"json-rpc-2-core", "json-rpc-2-transport"},
    "json-rpc-2-websockets": {
        "json-rpc-2-core", "json-rpc-2-transport", "json-rpc-2-tcp",
        "json-rpc-2-datagram", "json-rpc-2-quic", "json-rpc-2-iot",
    },
    "json-rpc-2-runtime": {
        "json-rpc-2-core", "json-rpc-2-transport", "json-rpc-2-server",
        "json-rpc-2-tcp", "json-rpc-2-datagram", "json-rpc-2-quic",
        "json-rpc-2-iot", "json-rpc-2-websockets",
    },
    "json-rpc-2": {
        "json-rpc-2-core", "json-rpc-2-transport", "json-rpc-2-client",
        "json-rpc-2-server", "json-rpc-2-runtime", "json-rpc-2-tcp",
        "json-rpc-2-datagram", "json-rpc-2-quic", "json-rpc-2-iot",
        "json-rpc-2-websockets",
    },
    "json-rpc-2-config": {"json-rpc-2"},
    "json-rpc-2-cli": {"json-rpc-2", "json-rpc-2-config"},
    "json-rpc-2-ffi": {"json-rpc-2"},
}
CORE_ALLOWED_EXTERNAL = {"bytes", "serde", "serde_json", "aes-gcm", "sha2"}


def violations(packages):
    names = {package["name"] for package in packages}
    errors = []
    for name in sorted(set(ALLOWED) - names):
        errors.append(f"Required architecture crate is missing: {name}")
    graph = {}
    for package in packages:
        name = package["name"]
        if name not in ALLOWED:
            errors.append(f"No architecture policy for workspace crate: {name}")
        graph[name] = set()
        for dependency in package["dependencies"]:
            if dependency.get("kind") == "dev":
                continue
            target = dependency["name"]
            if target in names:
                graph[name].add(target)
                if target not in ALLOWED.get(name, set()):
                    errors.append(f"Forbidden production dependency: {name} -> {target}")
            elif name == "json-rpc-2-core" and target not in CORE_ALLOWED_EXTERNAL:
                errors.append(f"Non-wire dependency in core: {target}")
    visited = set()
    active = set()

    def visit(name):
        if name in active:
            errors.append(f"Production dependency cycle through {name}")
            return
        if name in visited:
            return
        active.add(name)
        for target in sorted(graph[name]):
            visit(target)
        active.remove(name)
        visited.add(name)

    for name in sorted(graph):
        visit(name)
    return errors


def main():
    root = Path(__file__).resolve().parent.parent
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root, check=False, capture_output=True, text=True,
    )
    if result.returncode:
        print(result.stderr, file=sys.stderr, end="")
        return result.returncode
    metadata = json.loads(result.stdout)
    members = set(metadata["workspace_members"])
    packages = [package for package in metadata["packages"] if package["id"] in members]
    errors = violations(packages)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Architecture boundaries verified for {len(packages)} workspace crates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
