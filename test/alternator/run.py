#!/usr/bin/env python3
# Use the run_lib.py library from ../cql_pytest:
import sys
sys.path.insert(1, sys.path[0] + '/../cql_pytest')
import run_lib as run

import os
import requests
import time
import cassandra.cluster
import cassandra.auth

# When tests are to be run against AWS (the "--aws" option), it is not
# necessary to start Scylla at all. All we need to do is to run pytest.
if '--aws' in sys.argv:
    success = run.run_pytest(run.get_file_dir(__file__), sys.argv[1:])
    exit(0 if success else 1)

# check_alternator() below uses verify=False to accept self-signed SSL
# certificates but then we get scary warnings. This trick disables them:
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print('Scylla is: ' + run.scylla + '.')

# If the "--raft" option is given, switch to the experimental Raft-based
# implementation of schema operations. When the experimental feature becomes
# the default, we can drop this option.
extra_scylla_options = []
if '--raft' in sys.argv:
    sys.argv.remove('--raft')
    extra_scylla_options += ['--experimental-features=raft']

# run_alternator_cmd runs the same as run_scylla_cmd with *additional*
# parameters, so in particular both CQL and Alternator will be enabled.
# This is useful for us because we need CQL to setup authentication.
def run_alternator_cmd(pid, dir):
    (cmd, env) = run.run_scylla_cmd(pid, dir)
    ip = run.pid_to_ip(pid)
    cmd += [
        '--alternator-address', ip,
        '--alternator-enforce-authorization', '1',
        '--alternator-write-isolation', 'always_use_lwt',
        '--alternator-streams-time-window-s', '0',
        '--alternator-timeout-in-ms', '30000',
        '--alternator-ttl-period-in-seconds', '1',
        # Allow testing experimental features. Following issue #9467, we need
        # to add here specific experimental features as they are introduced.
        # We only list here Alternator-specific experimental features - CQL
        # ones are listed in test/cql_pytest/run_lib.py.
        '--experimental-features=alternator-streams',
        '--experimental-features=alternator-ttl',
    ]
    if '--https' in sys.argv:
        run.setup_ssl_certificate(dir)
        cmd += ['--alternator-https-port', '8043',
            '--alternator-encryption-options', f'keyfile={dir}/scylla.key',
            '--alternator-encryption-options', f'certificate={dir}/scylla.crt',
        ]
    else:
        cmd += ['--alternator-port', '8000']
    cmd += extra_scylla_options

    return (cmd, env)

pid = run.run_with_temporary_dir(run_alternator_cmd)
ip = run.pid_to_ip(pid)

if '--https' in sys.argv:
    alternator_url=f"https://{ip}:8043"
else:
    alternator_url=f"http://{ip}:8000"

# Wait for both CQL and Alternator APIs to become responsive. We obviously
# need the Alternator API to test Alternator, but currently we also need
# CQL for setting up authentication.
def check_alternator(url):
    try:
        requests.get(url, verify=False)
    except requests.ConnectionError:
        raise run.NotYetUp
    # Any other exception may indicate a problem, and is passed to the caller.

run.wait_for_services(pid, [
    lambda: run.check_cql(ip),
    lambda: check_alternator(alternator_url),
])

# Set up the the proper authentication credentials needed by the Alternator
# test. Currently this can only be done through CQL, which is why above we
# needed to make sure CQL is available.
cluster = run.get_cql_cluster(ip)
cluster.connect().execute("INSERT INTO system_auth.roles (role, salted_hash) VALUES ('alternator', 'secret_pass')")
cluster.shutdown()

# Finally run pytest:
success = run.run_pytest(run.get_file_dir(__file__), ['--url', alternator_url] + sys.argv[1:])

run.summary = 'Alternator tests pass' if success else 'Alternator tests failure'

exit(0 if success else 1)

# Note that the run.cleanup_all() function runs now, just like on any exit
# for any reason in this script. It will delete the temporary files and
# announce the failure or success of the test (printing run.summary).
