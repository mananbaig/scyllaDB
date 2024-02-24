# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for compaction strategy validation
#############################################################################

import pytest
from util import new_test_table
from cassandra.protocol import ConfigurationException

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int", "WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' }") as table:
        yield table

# NOTE: The following tests which use this assert_throws() all try to 
# check the specific wording of the error text, and it sometimes differs
# between Scylla and Cassandra - so we need to allow both: msg is a regular
# expression, so you can use the "|" character to allow two options.
def assert_throws(cql, table1, msg, cmd):
    with pytest.raises(ConfigurationException, match=msg):
        cql.execute(cmd.replace('%s', table1))

def test_common_options(cql, table1):
    assert_throws(cql, table1, r"tombstone_threshold value \(-0.4\) must be between 0.0 and 1.0|tombstone_threshold must be greater than 0, but was -0.400000", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'tombstone_threshold' : -0.4 }")
    assert_throws(cql, table1, r"tombstone_threshold value \(5.5\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'tombstone_threshold' : 5.5 }")
    assert_throws(cql, table1, r"tombstone_compaction_interval value \(-7000ms\) must be positive", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'tombstone_compaction_interval' : -7 }")

def test_size_tiered_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, r"min_sstable_size value \(-1\) must be non negative|min_sstable_size must be non negative: -1", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_sstable_size' : -1 }")
    assert_throws(cql, table1, r"bucket_low value \(0\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 0.0 }")
    assert_throws(cql, table1, r"bucket_low value \(1.3\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_low' : 1.3 }")
    assert_throws(cql, table1, r"bucket_high value \(0.7\) must be greater than 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'bucket_high' : 0.7 }")
    assert_throws(cql, table1, r"cold_reads_to_omit value \(-8.1\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'cold_reads_to_omit' : -8.1 }")
    assert_throws(cql, table1, r"cold_reads_to_omit value \(3.5\) must be between 0.0 and 1.0", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'cold_reads_to_omit' : 3.5 }")
    assert_throws(cql, table1, r"min_threshold value \(1\) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 1 }")

def test_time_window_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, "Invalid window unit SECONDS for compaction_window_unit|SECONDS is not valid for compaction_window_unit", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_unit' : 'SECONDS' }")
    assert_throws(cql, table1, r"compaction_window_size value \(-8\) must be greater than 1|-8 must be greater than 1 for compaction_window_size", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'compaction_window_size' : -8 }")
    assert_throws(cql, table1, r"enable_optimized_twcs_queries value \(no\) must be \"true\" or \"false\"", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'enable_optimized_twcs_queries' : 'no' }")
    assert_throws(cql, table1, r"max_threshold value \(1\) must be bigger or equal to 2", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'max_threshold' : 1 }")

def test_leveled_compaction_strategy_options(cql, table1):
    assert_throws(cql, table1, r"sstable_size_in_mb value \(-5\) must be positive|sstable_size_in_mb must be larger than 0, but was -5", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : -5 }")

def test_not_allowed_options(cql, table1):
    assert_throws(cql, table1, r"Invalid compaction strategy options {{abc, -54.54}} for chosen strategy type|Properties specified \[abc\] are not understood by SizeTieredCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'abc' : -54.54 }")
    assert_throws(cql, table1, r"Invalid compaction strategy options {{dog, 3}} for chosen strategy type|Properties specified \[dog\] are not understood by TimeWindowCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'TimeWindowCompactionStrategy', 'dog' : 3 }")
    assert_throws(cql, table1, r"Invalid compaction strategy options {{compaction_window_size, 4}} for chosen strategy type|Properties specified \[compaction_window_size\] are not understood by LeveledCompactionStrategy", "ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy', 'compaction_window_size' : 4 }")

def test_alter_table_with_twcs_timestamp_resolution_options(cql, table1):
    timestamp_resolutions = ["MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"]
    for tr in timestamp_resolutions:
        cql.execute(f"ALTER TABLE {table1} WITH compaction = {{ 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : '{tr}' }}")

    incorrect_timestamp_resolution = "YEARS"
    assert_throws(cql, table1, f"Invalid timestamp resolution {incorrect_timestamp_resolution} for timestamp_resolution", f"ALTER TABLE %s WITH compaction = {{ 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : '{incorrect_timestamp_resolution}' }}")

def test_create_table_with_twcs_timestamp_resolution_options(cql, test_keyspace):
    timestamp_resolutions = ["MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"]
    for tr in timestamp_resolutions:
        with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int", f"WITH compaction = {{ 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : '{tr}' }}") as table:
            pass

    incorrect_timestamp_resolution = "YEARS"
    with pytest.raises(ConfigurationException, match=f"Invalid timestamp resolution {incorrect_timestamp_resolution} for timestamp_resolution"):
        with new_test_table(cql, test_keyspace, "a int PRIMARY KEY, b int", f"WITH compaction = {{ 'class' : 'TimeWindowCompactionStrategy', 'timestamp_resolution' : '{incorrect_timestamp_resolution}' }}") as table:
            pass
