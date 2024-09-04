add_scylla_test(UUID_test
  KIND BOOST)
add_scylla_test(allocation_strategy_test
  KIND BOOST)
add_scylla_test(alternator_unit_test
  KIND BOOST
  LIBRARIES alternator)
add_scylla_test(anchorless_list_test
  KIND BOOST)
add_scylla_test(auth_passwords_test
  KIND BOOST
  LIBRARIES auth)
add_scylla_test(auth_resource_test
  KIND BOOST)
add_scylla_test(big_decimal_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(bloom_filter_test
  KIND SEASTAR)
add_scylla_test(bptree_test
  KIND SEASTAR
  LIBRARIES utils
  SOURCES
    bptree_test.cc
    tree_test_key.cc)
add_scylla_test(broken_sstable_test
  KIND SEASTAR)
add_scylla_test(btree_test
  KIND SEASTAR)
add_scylla_test(bytes_ostream_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(cache_mutation_reader_test
  KIND SEASTAR)
add_scylla_test(cached_file_test
  KIND SEASTAR)
add_scylla_test(caching_options_test
  KIND BOOST)
add_scylla_test(canonical_mutation_test
  KIND SEASTAR)
add_scylla_test(cartesian_product_test
  KIND BOOST)
add_scylla_test(cdc_generation_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(cell_locker_test
  KIND SEASTAR)
add_scylla_test(checksum_utils_test
  KIND BOOST)
add_scylla_test(chunked_managed_vector_test
  KIND SEASTAR)
add_scylla_test(chunked_vector_test
  KIND BOOST)
add_scylla_test(clustering_ranges_walker_test
  KIND SEASTAR)
add_scylla_test(compaction_group_test
  KIND SEASTAR)
add_scylla_test(compound_test
  KIND SEASTAR)
add_scylla_test(compress_test
  KIND BOOST)
add_scylla_test(config_test
  KIND SEASTAR)
add_scylla_test(continuous_data_consumer_test
  KIND SEASTAR)
add_scylla_test(counter_test
  KIND SEASTAR)
add_scylla_test(cql_auth_syntax_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(crc_test
  KIND BOOST)
add_scylla_test(dirty_memory_manager_test
  KIND SEASTAR)
add_scylla_test(double_decker_test
  KIND SEASTAR)
add_scylla_test(duration_test
  KIND BOOST)
add_scylla_test(dynamic_bitset_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(enum_option_test
  KIND BOOST)
add_scylla_test(enum_set_test
  KIND BOOST)
add_scylla_test(estimated_histogram_test
  KIND BOOST)
add_scylla_test(exception_container_test
  KIND SEASTAR)
add_scylla_test(exceptions_fallback_test
  KIND SEASTAR)
add_scylla_test(exceptions_optimized_test
  KIND SEASTAR)
add_scylla_test(expr_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(flush_queue_test
  KIND SEASTAR)
add_scylla_test(fragmented_temporary_buffer_test
  KIND SEASTAR)
add_scylla_test(frozen_mutation_test
  KIND SEASTAR)
add_scylla_test(generic_server_test
  KIND SEASTAR)
add_scylla_test(gossiping_property_file_snitch_test
  KIND SEASTAR)
add_scylla_test(hash_test
  KIND SEASTAR)
add_scylla_test(hashers_test
  KIND SEASTAR)
add_scylla_test(hint_test
  KIND SEASTAR)
add_scylla_test(idl_test
  KIND BOOST
  LIBRARIES idl)
add_scylla_test(index_reader_test
  KIND SEASTAR)
add_scylla_test(input_stream_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(intrusive_array_test
  KIND SEASTAR)
add_scylla_test(json_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(keys_test
  KIND BOOST
  LIBRARIES idl schema)
add_scylla_test(like_matcher_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(limiting_data_source_test
  KIND SEASTAR)
add_scylla_test(linearizing_input_stream_test
  KIND BOOST)
add_scylla_test(lister_test
  KIND SEASTAR)
add_scylla_test(log_heap_test
  KIND BOOST)
add_scylla_test(managed_bytes_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(map_difference_test
  KIND BOOST)
add_scylla_test(murmur_hash_test
  KIND BOOST)
add_scylla_test(nonwrapping_interval_test
  KIND BOOST)
add_scylla_test(observable_test
  KIND BOOST)
add_scylla_test(pretty_printers_test
  KIND BOOST)
add_scylla_test(range_tombstone_list_test
  KIND BOOST)
add_scylla_test(rust_test
  KIND BOOST
  LIBRARIES inc)
add_scylla_test(aws_errors_test
  KIND BOOST)
add_scylla_test(aws_error_injection_test
  KIND SEASTAR)
add_scylla_test(serialization_test
  KIND BOOST)
add_scylla_test(serialized_action_test
  KIND SEASTAR)
add_scylla_test(service_level_controller_test
  KIND SEASTAR)
add_scylla_test(small_vector_test
  KIND SEASTAR)
add_scylla_test(sstable_generation_test
  KIND BOOST)
add_scylla_test(string_format_test
  KIND BOOST)
add_scylla_test(summary_test
  KIND BOOST)
add_scylla_test(top_k_test
  KIND BOOST)
add_scylla_test(utf8_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(vint_serialization_test
  KIND BOOST
   LIBRARIES
     scylla-main
     utils)
add_scylla_test(wrapping_interval_test
  KIND BOOST)
add_scylla_test(address_map_test
  KIND SEASTAR)

add_scylla_test(combined_tests
  KIND SEASTAR
  SOURCES
    combined_tests.cc
    aggregate_fcts_test.cc
    auth_test.cc
    batchlog_manager_test.cc
    cache_algorithm_test.cc
    castas_fcts_test.cc
    cdc_test.cc
    column_mapping_test.cc
    commitlog_cleanup_test.cc
    commitlog_test.cc
    cql_auth_query_test.cc
    cql_functions_test.cc
    cql_query_group_test.cc
    cql_query_large_test.cc
    cql_query_like_test.cc
    cql_query_test.cc
    database_test.cc
    data_listeners_test.cc
    error_injection_test.cc
    extensions_test.cc
    filtering_test.cc
    group0_cmd_merge_test.cc
    group0_test.cc
    index_with_paging_test.cc
    json_cql_query_test.cc
    large_paging_state_test.cc
    tree_test_key.cc
  LIBRARIES
    cql3
    idl
    tools
    utils)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/suite.yaml")
  set(scylla_tests "${scylla_tests}" PARENT_SCOPE)
endif()
