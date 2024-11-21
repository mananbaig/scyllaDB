#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import re
import locale
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from test.dtest.ccmlib.common import ArgumentError

if TYPE_CHECKING:
    from test.pylib.internal_types import ServerInfo
    from test.dtest.ccmlib.scylla_cluster import ScyllaCluster


NODETOOL_STDERR_IGNORED_PATTERNS = (
    re.compile("WARNING: debug mode. Not for benchmarking or production"),
    re.compile(
        "==[0-9]+==WARNING: ASan doesn't fully support makecontext/swapcontext"
        " functions and may produce false positives in some cases!"
    ),
)


class ToolError(Exception):
    def __init__(self, command: str, exit_status: int, stdout: Any = None, stderr: Any = None):
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr

        message = [f"Subprocess {command} exited with non-zero status; exit status: {exit_status}"]
        if stdout:
            message.append(f"stdout: {self._decode(stdout)}")
        if stderr:
            message.append(f"stderr: {self._decode(stderr)}")

        Exception.__init__(self, "; \n".join(message))

    @staticmethod
    def _decode(value: str | bytes) -> str:
        if isinstance(value, bytes):
            return bytes.decode(value, locale.getpreferredencoding(do_setlocale=False))
        return value


NodetoolError = ToolError


class ScyllaNode:
    def __init__(self, cluster: ScyllaCluster, server: ServerInfo):
        self.cluster = cluster
        self.server_id = server.server_id
        self.pid = None
        self.all_pids = []
        self.network_interfaces = {
            "storage": (str(server.rpc_address), 7000),
            "binary": (str(server.rpc_address), 9042),
        }
        self.data_center = server.datacenter
        self.rack = server.rack

    @property
    def name(self) -> str:
        return f"node{self.server_id}"

    def address(self) -> str:
        """Return the IP use by this node for internal communication."""

        return self.network_interfaces["storage"][0]

    def grep_log(self, expr, filter_expr=None, filename='system.log', from_mark=None):  # TODO: implement this
        return []

    def grep_log_for_errors(self, filename='system.log', distinct_errors=False, search_str=None, case_sensitive=True, from_mark=None):  # TODO: implement this
        return []

    def is_running(self) -> bool:
        return any(self.server_id == s.server_id for s in self.cluster.manager.running_servers())

    def decommission(self) -> None:
        self.cluster.manager.decommission_node(server_id=self.server_id)

    def start(self, wait_for_binary_proto: bool | None = None, wait_other_notice: bool | None = None) -> None:
        self.cluster.manager.server_start(server_id=self.server_id)

    def rmtree(self, path: str | Path) -> None:
        """Delete a directory content without removing the directory.

        Copied this code from Python's documentation for Path.walk() method.
        """
        for root, dirs, files in Path(path).walk(top_down=False):
            for name in files:
                (root / name).unlink()
            for name in dirs:
                (root / name).rmdir()

    def nodetool(self,
                 cmd: str,
                 capture_output: bool = True,
                 wait: bool = True,
                 timeout: int | float | None = None,
                 verbose: bool = True) -> tuple[str, str]:
        if capture_output and not wait:
            raise ArgumentError("Cannot set capture_output while wait is False.")

        nodetool_cmd = [
            self.cluster.manager.server_get_exe(server_id=self.server_id),
            "nodetool",
            "-h",
            str(self.cluster.manager.get_host_ip(server_id=self.server_id)),
            *cmd.split(),
        ]

        if verbose:
            self.debug(f"nodetool cmd={nodetool_cmd} wait={wait} timeout={timeout}")

        if capture_output:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate(timeout=timeout)
        else:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True)
            stdout, stderr = None, None

        if wait and p.wait(timeout=timeout):
            raise NodetoolError(" ".join(nodetool_cmd), p.returncode, stdout, stderr)

        stderr = "\n".join(
            line for line in stderr.splitlines()
            if self.debug(f"checking {line}") or not any(p.fullmatch(line) for p in NODETOOL_STDERR_IGNORED_PATTERNS)
        )

        return stdout, stderr

    def _log_message(self, message: str) -> str:
        return f"{self.name}: {message}"

    def debug(self, message: str) -> None:
        self.cluster.debug(self._log_message(message))

    def info(self, message: str) -> None:
        self.cluster.info(self._log_message(message))

    def warning(self, message: str) -> None:
        self.cluster.warning(self._log_message(message))

    def error(self, message: str) -> None:
        self.cluster.error(self._log_message(message))

    def get_path(self) -> str:
        """Return the path to this node top level directory (where config/data is stored.)"""

        return self.cluster.manager.server_get_workdir(server_id=self.server_id)

    def repair(self, options: list[str] | None = None, **kwargs) -> tuple[str, str]:
        cmd = ["repair"]
        if options:
            cmd.extend(options)
        return self.nodetool(" ".join(cmd), **kwargs)

    def drain(self, block_on_log: bool = False) -> None:
        # mark = self.mark_log()
        self.nodetool("drain")
        # if block_on_log:
        #     self.watch_log_for("DRAINED", from_mark=mark)

    def flush(self, ks: str | None = None, table: str | None = None, **kwargs) -> None:
        cmd = ["flush"]
        if ks:
            cmd.append(ks)
        if table:
            cmd.append(table)
        self.nodetool(" ".join(cmd), **kwargs)

    def stop(self, wait: bool = True, wait_other_notice: bool = False, other_nodes=None, gently: bool = True, wait_seconds: int = 127) -> bool:
        self.cluster.manager.server_stop_gracefully(server_id=self.server_id)

    def __repr__(self) -> str:
        return f"<ScyllaNode name={self.server_id} dc={self.data_center} rack={self.rack}>"
