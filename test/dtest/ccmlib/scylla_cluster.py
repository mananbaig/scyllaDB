#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

from test.pylib.manager_client import ManagerClient
from test.dtest.ccmlib.scylla_node import ScyllaNode


class ScyllaCluster:
    scylla_mode = "debug"  # TODO: implement this

    def __init__(self, manager: ManagerClient):
        self.manager = manager
        self._config_options = {}

    @property
    def nodes(self) -> dict[str, ScyllaNode]:
        return {node.name: node for node in self.nodelist()}

    def nodelist(self) -> list[ScyllaNode]:
        return [ScyllaNode(cluster=self, server=server) for server in self.manager.all_servers()]

    def populate(self, nodes: int) -> ScyllaCluster:
        self.manager.servers_add(servers_num=nodes, config=self._config_options, start=False)
        return self

    def start(self, wait_for_binary_proto: bool | None = None, wait_other_notice: bool | None = None) -> None:
        for server in self.manager.all_servers():
            self.manager.server_start(server_id=server.server_id)

    def stop(self, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=127):
        for server in self.manager.running_servers():
            self.manager.server_stop(server_id=server.server_id)

    def version(self) -> str:  # TODO: implement this
        return "6.3.0"

    def set_configuration_options(self,
                                  values: dict | None = None,
                                  batch_commitlog: bool | None = None) -> ScyllaCluster:
        values = {} if values is None else values.copy()
        if batch_commitlog is not None:
            if batch_commitlog:
                values["commitlog_sync"] = "batch"
                values["commitlog_sync_batch_window_in_ms"] = 5
                values["commitlog_sync_period_in_ms"] = None
            else:
                values["commitlog_sync"] = "periodic"
                values["commitlog_sync_period_in_ms"] = 10000
                values["commitlog_sync_batch_window_in_ms"] = None
        if values:
            self._config_options.update(values)
            for server in self.manager.all_servers():
                for k, v in values.items():
                    self.manager.server_update_config(server_id=server.server_id, key=k, value=v)
        return self
