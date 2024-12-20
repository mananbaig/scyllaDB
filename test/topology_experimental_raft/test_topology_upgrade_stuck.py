#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver, restart, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@log_run_time
async def test_topology_upgrade_stuck(request, manager: ManagerClient):
    """
    Simulates a situation where upgrade procedure gets stuck due to majority
    loss: we have one upgraded node, one not upgraded node, and three nodes
    permanently down. Then, it verifies that it's possible to perform recovery
    procedure and redo the upgrade after the issue is resolved.
    """

    # First, force the first node to start in legacy mode
    cfg = {'force_gossip_topology_changes': True}

    servers = [await manager.server_add(config=cfg) for _ in range(5)]
    to_be_upgraded_node, to_be_isolated_node, *to_be_shutdown_nodes = servers
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Enabling error injection which will cause the topology coordinator to get stuck")
    await asyncio.gather(*(manager.api.enable_injection(s.ip_addr, "topology_coordinator_fail_to_build_state_during_upgrade", one_shot=False) for s in servers))

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade gets stuck due to error injection")
    logs = await asyncio.gather(*(manager.server_open_log(s.server_id) for s in servers))
    log_watch_tasks = [asyncio.create_task(l.wait_for("failed to build topology coordinator state due to error injection")) for l in logs]
    _, pending = await asyncio.wait(log_watch_tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    logging.info("Isolate one of the nodes via error injection")
    await manager.api.enable_injection(to_be_isolated_node.ip_addr, "raft_drop_incoming_append_entries", one_shot=False)

    logging.info("Disable the error injection that causes upgrade to get stuck")
    await asyncio.gather(*(manager.api.disable_injection(s.ip_addr, "topology_coordinator_fail_to_build_state_during_upgrade") for s in servers))

    logging.info("Shut down three nodes to simulate quorum loss")
    await asyncio.gather(*(manager.server_stop(s.server_id) for s in to_be_shutdown_nodes))

    logging.info("Disable the error injection that causes node to be isolated")
    await manager.api.disable_injection(to_be_isolated_node.ip_addr, "raft_drop_incoming_append_entries")

    logging.info("Checking that not all nodes finished upgrade")
    upgraded_count = 0
    for s in [to_be_upgraded_node, to_be_isolated_node]:
        status = await manager.api.raft_topology_upgrade_status(s.ip_addr)
        if status == "done":
            upgraded_count += 1
    assert upgraded_count != 2

    logging.info(f"Only {upgraded_count}/2 nodes finished upgrade, which was expected")

    servers, others = [to_be_upgraded_node, to_be_isolated_node], to_be_shutdown_nodes

    logging.info(f"Obtaining hosts for nodes {servers}")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Restarting hosts {hosts} in recovery mode")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    for srv in servers:
        await restart(manager, srv)
    cql = await reconnect_driver(manager)

    await manager.servers_see_each_other(servers)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f"Driver reconnected, hosts: {hosts}")

    for i in range(len(others)):
        to_remove = others[i]
        ignore_dead_ips = [srv.ip_addr for srv in others[i+1:]]
        logging.info(f"Removing {to_remove} using {servers[0]} with ignore_dead: {ignore_dead_ips}")
        await manager.remove_node(servers[0].server_id, to_remove.server_id, ignore_dead_ips)

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_topology_state(cql, h) for h in hosts))
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting hosts {hosts}")
    for srv in servers:
        await restart(manager, srv)
    cql = await reconnect_driver(manager)

    logging.info("Cluster restarted, waiting until driver reconnects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting until upgrade to raft schema finishes")
    await asyncio.gather(*(wait_until_schema_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logging.info("Checking the topology upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    logging.info("Waiting until all nodes see others as alive")
    await manager.servers_see_each_other(servers)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Waiting for CDC generations publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logging.info("Booting three new nodes")
    servers += await asyncio.gather(*(manager.server_add() for _ in range(3)))
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the new CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, hosts, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
