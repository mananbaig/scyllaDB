#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from copy import copy
from pathlib import Path, PosixPath

import yaml
from pytest import Collector

from test.pylib.cpp.facade import CppTestFacade
from test.pylib.cpp.item import CppFile
from test.pylib.util import get_configured_modes

ALL_MODES = {
    'debug': 'Debug',
    'release': 'RelWithDebInfo',
    'dev': 'Dev',
    'sanitize': 'Sanitize',
    'coverage': 'Coverage',
}
DEBUG_MODES = {
    'debug': 'Debug',
    'sanitize': 'Sanitize',
}
DEFAULT_ARGS = [
    '--overprovisioned',
    '--unsafe-bypass-fsync 1',
    '--kernel-page-cache 1',
    '--blocked-reactor-notify-ms 2000000',
    '--collectd 0',
    '--max-networking-io-control-blocks=100',
]


def get_disabled_tests(config: dict, modes: [str]) -> dict[str, set[str]]:
    """
    Get the dict with disabled tests.
    Pytest spawns one process, so all modes should be handled there instead one by one as test.py does.
    """
    disabled_tests = {}
    for mode in modes:
        # Skip tests disabled in suite.yaml
        disabled_tests_for_mode = set(config.get('disable', []))
        # Skip tests disabled in the specific mode.
        disabled_tests_for_mode.update(config.get('skip_in_' + mode, []))
        # If this mode is one of the debug modes, and there are
        # tests disabled in a debug mode, add these tests to the skip list.
        if mode in DEBUG_MODES:
            disabled_tests_for_mode.update(config.get('skip_in_debug_modes', []))
        # If a test is listed in run_in_<mode>, it should only be enabled in
        # this mode. Tests not listed in any run_in_<mode> directive should
        # run in all modes. Inverting this, we should disable all tests
        # that are listed explicitly in some run_in_<m> where m != mode
        #  This, of course, may create ambiguity with skip_* settings,
        # since the priority of the two is undefined, but oh well.
        run_in_m = set(config.get('run_in_' + mode, []))
        for a in ALL_MODES:
            if a == mode:
                continue
            skip_in_m = set(config.get('run_in_' + a, []))
            disabled_tests_for_mode.update(skip_in_m - run_in_m)
        disabled_tests[mode] = disabled_tests_for_mode
    return disabled_tests


def read_suite_config(directory: Path) -> dict[str, str]:
    """
    Helper method that will return the configuration from the suite.yaml file
    """
    with open(directory / 'suite.yaml', 'r') as cfg_file:
        cfg = yaml.safe_load(cfg_file.read())
        if not isinstance(cfg, dict):
            raise RuntimeError('Failed to load tests: suite.yaml is empty')
        return cfg


def get_modes_to_run(session) -> list[str]:
    modes = session.config.getoption('modes')
    if not modes:
        modes = get_configured_modes()
    if not modes:
        raise RuntimeError('No modes configured. Please run ./configure.py first')
    return modes


def collect_items(file_path: PosixPath, parent: Collector, facade: CppTestFacade, run_id=None) -> object:
    """
    Collect c++ test based on the .cc files. C++ test binaries are located in different directory, so the method will take care
    to provide the correct path to the binary based on the file name and mode.
    """
    modes = get_modes_to_run(parent.session)
    suite_config = read_suite_config(file_path.parent)
    no_parallel_cases = suite_config.get('no_parallel_cases', [])
    disabled_tests = get_disabled_tests(suite_config, modes)
    args = copy(DEFAULT_ARGS)
    custom_args_config = suite_config.get('custom_args', {})
    test_name = file_path.stem
    no_parallel_run = True if test_name in no_parallel_cases else False

    custom_args = custom_args_config.get(file_path.stem, ['-c2 -m2G'])
    if len(custom_args) > 1:
        return CppFile.from_parent(parent=parent, path=file_path, arguments=args, parameters=custom_args,
                                   no_parallel_run=no_parallel_run, modes=modes, disabled_tests=disabled_tests,
                                   run_id=run_id, facade=facade)
    else:
        args.extend(custom_args)
        return CppFile.from_parent(parent=parent, path=file_path, arguments=args, no_parallel_run=no_parallel_run,
                                   modes=modes, disabled_tests=disabled_tests, run_id=run_id, facade=facade)
