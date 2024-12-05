#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import logging
import os
import pprint
import re
from functools import partial, partialmethod
from typing import TYPE_CHECKING

import requests
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable, default_lbp_factory
from cassandra.cluster import Cluster as PyCluster
from cassandra.policies import ExponentialReconnectionPolicy, WhiteListRoundRobinPolicy
from packaging.version import Version

from test.dtest.dtest_class import (
    get_auth_provider,
    get_eager_protocol_version,
    get_ip_from_node,
    get_port_from_node,
    make_execution_profile,
)
from test.dtest.ccmlib.scylla_cluster import ScyllaCluster
from test.dtest.tools.context import log_filter
from test.dtest.tools.log_utils import DisableLogger, remove_control_chars
from test.dtest.tools.misc import retry_till_success

if TYPE_CHECKING:
    from test.dtest.dtest_config import DTestConfig
    from test.dtest.dtest_setup_overrides import DTestSetupOverrides
    from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)

# Add custom TRACE level, for development print we don't want on debug level
logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


class DTestSetup:
    def __init__(self,
                 dtest_config: DTestConfig | None = None,
                 setup_overrides: DTestSetupOverrides | None = None,
                 manager: ManagerClient | None = None,
                 scylla_mode: str | None = None,
                 cluster_name: str = "test"):
        self.dtest_config = dtest_config
        self.setup_overrides = setup_overrides
        self.cluster_name = cluster_name
        self.ignore_log_patterns = []
        self.ignore_cores_log_patterns = []
        self.ignore_cores = []
        self.cluster = ScyllaCluster(manager=manager, scylla_mode=scylla_mode)
        self.cluster_options = {}
        self.replacement_node = None
        self.allow_log_errors = False
        self.connections = []
        self.jvm_args = []
        self.base_cql_timeout = 10  # seconds
        self.cql_request_timeout = None
        self.scylla_features: set[str] = self.dtest_config.scylla_features

    def find_cores(self):
        cores = []
        ignored_cores = []
        nodes = []
        for node in self.cluster.nodelist():
            try:
                pids = node.all_pids
                if not pids:
                    pids = [node.pid]
            except AttributeError:
                pids = [node.pid]
            nodes += [(node, pids)]
        for f in os.listdir("."):
            if not f.endswith(".core"):
                continue
            for node, pids in nodes:
                """Look for this cluster's coredumps"""
                for p in pids:
                    if f.find(f".{p}.") >= 0:
                        path = os.path.join(os.getcwd(), f)
                        if not node in self.ignore_cores:
                            cores += [(node.name, path)]
                        else:
                            logger.debug(f"Ignoring core file {path} belonging to {node.name} due to ignore_cores_log_patterns")
                            ignored_cores += [(node.name, path)]
        # returns empty list if no core files found
        return cores, ignored_cores

    def cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        return self._create_session(node, keyspace, user, password, compression, protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def cql_cluster_session(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        topology_event_refresh_window=10,
        request_timeout=None,
        exclusive=False,
        **kwargs,
    ):
        if exclusive:
            node_ip = get_ip_from_node(node)
            topology_event_refresh_window = -1
            load_balancing_policy = WhiteListRoundRobinPolicy([node_ip])
        else:
            load_balancing_policy = default_lbp_factory()

        session = self._create_session(
            node,
            keyspace,
            user,
            password,
            compression,
            protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            load_balancing_policy=load_balancing_policy,
            request_timeout=request_timeout,
            keep_session=False,
            **kwargs,
        )

        class ClusterSession:
            def __init__(self, session):
                self.session = session

            def __del__(self):
                self.__cleanup()

            def __enter__(self):
                return self.session

            def __exit__(self, _type, value, traceback):
                self.__cleanup()

            def __cleanup(self):
                if self.session:
                    self.session.cluster.shutdown()
                    self.session = None

        return ClusterSession(session)

    def patient_cql_cluster_session(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        request_timeout=None,
        compression=True,
        timeout=60,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        topology_event_refresh_window=10,
        exclusive=False,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(
            self.cql_cluster_session,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            request_timeout=request_timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            exclusive=exclusive,
            bypassed_exception=NoHostAvailable,
            **kwargs,
        )

    def exclusive_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression, protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr, **kwargs)

    def _create_session(  # noqa: PLR0913
        self,
        node,
        keyspace,
        user,
        password,
        compression,
        protocol_version,
        port=None,
        ssl_opts=None,
        execution_profiles=None,
        topology_event_refresh_window=10,
        request_timeout=None,
        keep_session=True,
        ssl_context=None,
        load_balancing_policy=None,
        **kwargs,
    ):
        nodes = []
        if type(node) is list:
            nodes = node
            node = nodes[0]
        else:
            nodes = [node]
        node_ips = [get_ip_from_node(node) for node in nodes]
        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = get_eager_protocol_version(node.cluster.version())

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        if request_timeout is None:
            request_timeout = self.cql_request_timeout

        if load_balancing_policy is None:
            load_balancing_policy = default_lbp_factory()

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(request_timeout=request_timeout, load_balancing_policy=load_balancing_policy, **kwargs)}
        if execution_profiles is not None:
            profiles.update(execution_profiles)

        cluster = PyCluster(
            node_ips,
            auth_provider=auth_provider,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_options=ssl_opts,
            connect_timeout=5,
            max_schema_agreement_wait=60,
            control_connection_timeout=6.0,
            allow_beta_protocol_version=True,
            topology_event_refresh_window=topology_event_refresh_window,
            execution_profiles=profiles,
            ssl_context=ssl_context,
            # The default reconnection policy has a large maximum interval
            # between retries (600 seconds). In tests that restart/replace nodes,
            # where a node can be unavailable for an extended period of time,
            # this can cause the reconnection retry interval to get very large,
            # longer than a test timeout.
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 4.0),
        )
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        if keep_session:
            self.connections.append(session)

        return session

    def patient_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        timeout=30,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        expected_log_lines = ("Control connection failed to connect, shutting down Cluster:", "[control connection] Error connecting to ")
        with log_filter("cassandra.cluster", expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs,
            )

        return session

    def patient_exclusive_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        timeout=30,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs,
        )

    def check_errors(self, node, exclude_errors=None, search_str=None, from_mark=None, regex=False, return_errors=False):  # noqa: PLR0913
        if from_mark != None:
            node.error_mark = from_mark
        errors = node.grep_log_for_errors(distinct_errors=True, search_str=search_str)

        if exclude_errors:
            if isinstance(exclude_errors, tuple):
                exclude_errors = list(exclude_errors)
            if not isinstance(exclude_errors, list):
                exclude_errors = [exclude_errors]
            if not regex:
                exclude_errors = [re.escape(ee) for ee in list(exclude_errors)]
        errors = list(self.__filter_errors(errors, exclude_errors))

        if errors:
            if not return_errors:
                assert False, "\n".join(list(errors))

        if return_errors:
            return list(errors)

        if exclude_errors:
            self.ignore_log_patterns += exclude_errors

    def check_errors_all_nodes(self, nodes=None, exclude_errors=None, search_str=None, regex=False):
        if nodes is None:
            nodes = self.cluster.nodelist()

        critical_errors = []
        found_errors = []
        for node in nodes:
            try:
                critical_errors_pattern = r"Assertion.*failed|AddressSanitizer"
                if self.ignore_cores_log_patterns:
                    expr = "|".join([f"({p})" for p in set(self.ignore_cores_log_patterns)])
                    matches = node.grep_log(expr)
                    if matches:
                        logger.debug(f"Will ignore cores on {node.name}. Found the following log messages: {matches}")
                        self.ignore_cores.append(node)
                if node not in self.ignore_cores:
                    critical_errors_pattern += "|Aborting on shard"
                filter_expr = "|".join(self.ignore_log_patterns)
                matches = node.grep_log(critical_errors_pattern, filter_expr=filter_expr)
                if matches:
                    critical_errors.append((node.name, [m[0].strip() for m in matches]))
            except FileNotFoundError:
                pass
            logger.debug(f"exclude_errors: {exclude_errors}")
            errors = self.check_errors(node=node, exclude_errors=exclude_errors, search_str=search_str, regex=regex, return_errors=True)
            if len(errors):
                found_errors.append((node.name, errors))

        if critical_errors:
            raise AssertionError(f"Critical errors found: {critical_errors}\nOther errors: {found_errors}")
        if found_errors:
            logger.error(f"Unexpected errors found: {found_errors}")
            errors_summary = "\n".join([f"{node}: {len(errors)} errors\n" + "\n".join(errors[:5]) for node, errors in found_errors])
            raise AssertionError(f"Unexpected errors found:\n{errors_summary}")
        found_cores, ignored_cores = self.find_cores()
        if found_cores:
            raise AssertionError("Core file(s) found. Marking test as failed.")

    def __filter_errors(self, errors, patterns=None):
        """Filter errors, removing those that match patterns"""
        if not patterns:
            patterns = []
        patterns += self.ignore_log_patterns
        patterns += self.ignore_cores_log_patterns
        patterns += [
            r"Compaction for .* deliberately stopped",
            r"update compaction history failed:.*ignored",
        ]
        # ignore expected rpc errors when nodes are stopped.
        expected_rpc_errors = [
            "connection dropped",
            "fail to connect",
        ]
        # we may stop nodes that have not finished starting yet
        patterns += [
            r"(Startup|start) failed:.*(seastar::sleep_aborted|raft::request_aborted)",
            r"Timer callback failed: seastar::gate_closed_exception",
        ]
        patterns += ["rpc - client .*({})".format("|".join(expected_rpc_errors))]
        # We see benign rpc errors when nodes start/stop.
        # If they cause system malfunction, it should be detected using higher-level tests.
        patterns += [r"rpc::unknown_verb_error"]
        patterns += ["raft_rpc - Failed to send", r"raft_topology.*(seastar::broken_promise|rpc::closed_error)"]

        # Expected tablet migration stream failure where a node is stopped.
        # refs: https://github.com/scylladb/scylladb/issues/19640
        patterns += [r"Failed to handle STREAM_MUTATION_FRAGMENTS.*rpc::stream_closed"]

        # Expected RAFT errors on decommission-abort or node restart with MV.
        patterns += [r"raft_topology - raft_topology_cmd.*failed with: raft::request_aborted"]

        pattern = re.compile("|".join([f"({p})" for p in set(patterns)]))
        for e in errors:
            if not pattern.search(e):
                yield remove_control_chars(e)

    def supports_v5_protocol(self, cluster_version):
        return cluster_version >= Version("4.0")

    def init_default_config(self):  # noqa: PLR0912
        # the failure detector can be quite slow in such tests with quick start/stop
        phi_values = {"phi_convict_threshold": 5}
        tasks_values = dict()

        cassandra_v4_cluster = not isinstance(self.cluster, ScyllaCluster) and self.cluster.version() >= "4"

        # enable read time tracking of repaired data between replicas by default
        if cassandra_v4_cluster:
            repaired_data_tracking_values = dict(repaired_data_tracking_for_partition_reads_enabled="true", repaired_data_tracking_for_range_reads_enabled="true", report_unconfirmed_repaired_data_mismatches="true")
        else:
            repaired_data_tracking_values = {}

        timeout = self.cql_timeout() * 1000
        range_timeout = 3 * timeout
        self.cql_request_timeout = 3 * self.cql_timeout()
        # count(*) queries are particularly slow in debug mode
        # need to adjust the session or query timeout respectively
        self.count_request_timeout = self.cql_timeout(400)

        if isinstance(self.cluster, ScyllaCluster):
            tasks_values = {"task_ttl_in_seconds": 0}
            logger.debug(f"Scylla mode is '{self.cluster.scylla_mode}'")
        logger.debug(f"Cluster *_request_timeout_in_ms={timeout}, range_request_timeout_in_ms={range_timeout}, cql request_timeout={self.cql_request_timeout}")

        values = self.cluster_options or dict()

        if not cassandra_v4_cluster:
            values = {
                **values,
                **phi_values,
                **tasks_values,
                **repaired_data_tracking_values,
                **dict(
                    read_request_timeout_in_ms=timeout,
                    range_request_timeout_in_ms=range_timeout,
                    write_request_timeout_in_ms=timeout,
                    truncate_request_timeout_in_ms=range_timeout,
                    counter_write_request_timeout_in_ms=timeout * 2,
                    cas_contention_timeout_in_ms=timeout,
                    request_timeout_in_ms=timeout,
                ),
            }
        else:
            values = {
                **values,
                **phi_values,
                **repaired_data_tracking_values,
                **dict(
                    read_request_timeout=f"{timeout}ms",
                    range_request_timeout=f"{range_timeout}ms",
                    write_request_timeout=f"{timeout}ms",
                    truncate_request_timeout=f"{range_timeout}ms",
                    counter_write_request_timeout=f"{timeout * 2}ms",
                    cas_contention_timeout=f"{timeout}ms",
                    request_timeout=f"{timeout}ms",
                ),
            }

        if self.setup_overrides is not None and len(self.setup_overrides.cluster_options) > 0:
            values = {**values, **self.setup_overrides.cluster_options}

        # No more thrift in 4.0, and start_rpc doesn't exists anymore
        if cassandra_v4_cluster:
            if "start_rpc" in values:
                del values["start_rpc"]
            values["corrupted_tombstone_strategy"] = "exception"

        if self.dtest_config.use_vnodes:
            self.cluster.set_configuration_options(values={"initial_token": None, "num_tokens": self.dtest_config.num_tokens})
        else:
            self.cluster.set_configuration_options(values={"num_tokens": None})

        if self.dtest_config.experimental_features:
            experimental_features = values.setdefault("experimental_features", [])
            for f in self.dtest_config.experimental_features:
                if f not in experimental_features:
                    experimental_features.append(f)
        self.scylla_features |= set(values.get("experimental_features", []))

        if isinstance(self.cluster, ScyllaCluster):
            if self.dtest_config.force_gossip_topology_changes:
                logger.debug("Forcing gossip topology changes")
                values["force_gossip_topology_changes"] = True

            logger.debug("Setting 'enable_tablets' to %s", self.dtest_config.tablets)
            values["enable_tablets"] = self.dtest_config.tablets
            if self.dtest_config.tablets:
                self.scylla_features.add("tablets")

        self.cluster.set_configuration_options(values)
        logger.debug("Done setting configuration options:\n" + pprint.pformat(self.cluster._config_options, indent=4))

    def cql_timeout(self, seconds=None):
        if not seconds:
            seconds = self.base_cql_timeout
        factor = 1
        if isinstance(self.cluster, ScyllaCluster):
            if self.cluster.scylla_mode == "debug":
                factor = 3
            elif self.cluster.scylla_mode != "release":
                factor = 2
        return seconds * factor

    def disable_error(self, name, node):
        """Disable error injection
        Args:
            name (str): name of error injection to be disabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Disabling error injection "{name}" on node {node_ip}')

            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def check_error(self, name, node):
        """Get status of error injection

        Args:
            name (str): name of error injection.
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def list_errors(self, node):
        """List enabled error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()
            return response.json()

    def disable_errors(self, node):
        """Disable all error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f"Disable all error injections on node {node_ip}")
            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()

    def enable_error(self, name, node, one_shot=False):
        """Enable error injection

        Args:
            name (str): name of error injection to be enabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
            one_shot (bool): indicates whether the injection is one-shot
                             (resets enabled state after triggering the injection).

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Enabling error injection "{name}" on node {node_ip}')
            response = requests.post(f"http://{node_ip}:10000/v2/error_injection/injection/{name}", params={"one_shot": one_shot})
            response.raise_for_status()
