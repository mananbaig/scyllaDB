#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from test.pylib.internal_types import ServerInfo
    from test.dtest.ccmlib.scylla_cluster import ScyllaCluster


class ScyllaNode:
    def __init__(self, cluster: ScyllaCluster, server: ServerInfo):
        self.cluster = cluster
        self.server_id = server.server_id
        self.pid = None
        self.all_pids = []
        self.network_interfaces = {
            "storage": (server.rpc_address, 7000),
            "binary": (server.rpc_address, 9042),
        }

    @property
    def name(self) -> str:
        return f"node{self.server_id}"

    def grep_log(self, expr, filter_expr=None, filename='system.log', from_mark=None):  # TODO: implement this
        return []

    def grep_log_for_errors(self, filename='system.log', distinct_errors=False, search_str=None, case_sensitive=True, from_mark=None):  # TODO: implement this
        return []

    def is_running(self) -> bool:
        return any(self.server_id == s.server_id for s in self.cluster.manager.running_servers())

    def decommission(self) -> None:
        self.cluster.manager.decommission_node(server_id=self.server_id)
