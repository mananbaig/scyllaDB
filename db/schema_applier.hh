/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "frozen_schema.hh"
#include "mutation/mutation.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "query-result-set.hh"
#include "db/schema_tables.hh"
#include "schema/schema_registry.hh"

#include <seastar/core/distributed.hh>
#include <unordered_map>

namespace db {

namespace schema_tables {

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload = false);

enum class table_kind { table, view };

struct table_selector {
    bool all_in_keyspace = false; // If true, selects all existing tables in a keyspace plus what's in "tables";
    std::unordered_map<table_kind, std::unordered_set<sstring>> tables;

    table_selector& operator+=(table_selector&& o);
    void add(table_kind t, sstring name);
    void add(sstring name);
};

struct schema_persisted_state {
    schema_tables::schema_result keyspaces;
    schema_tables::schema_result scylla_keyspaces;
    std::map<table_id, schema_mutations> tables;
    schema_tables::schema_result types;
    std::map<table_id, schema_mutations> views;
    schema_tables::schema_result functions;
    schema_tables::schema_result aggregates;
    schema_tables::schema_result scylla_aggregates;
};

struct affected_keyspaces_names {
    std::set<sstring> created;
    std::set<sstring> altered;
    std::set<sstring> dropped;
};

// groups keyspaces based on what is happening to them during schema change
struct affected_keyspaces {
    std::vector<replica::database::created_keyspace_per_shard> created;
    std::vector<replica::database::keyspace_change_per_shard> altered;
    // names need to be copied here as they are used multiple times and
    // keyspace struct from which we obtain the name is moved when
    // we commit it
    affected_keyspaces_names names;
};

struct affected_user_types_per_shard {
    std::vector<user_type> created;
    std::vector<user_type> altered;
    std::vector<user_type> dropped;
};

// groups UDTs based on what is happening to them during schema change
using affected_user_types = std::vector<affected_user_types_per_shard>;

struct frozen_schema_diff {
    struct altered_schema {
        frozen_schema old_schema;
        frozen_schema new_schema;
    };
    std::vector<frozen_schema> created;
    std::vector<altered_schema> altered;
    std::vector<frozen_schema> dropped;
};

// schema_diff represents what is happening with tables or views during schema merge
struct schema_diff_per_shard {
    struct altered_schema {
        schema_ptr old_schema;
        schema_ptr new_schema;
    };

    std::vector<schema_ptr> created;
    std::vector<altered_schema> altered;
    std::vector<schema_ptr> dropped;

    future<frozen_schema_diff> freeze() const;

    static future<foreign_ptr<std::unique_ptr<schema_diff_per_shard>>> copy_from(replica::database&, std::shared_ptr<data_dictionary::user_types_storage>, const frozen_schema_diff& oth);
};

struct affected_tables_and_views {
    std::vector<foreign_ptr<std::unique_ptr<schema_diff_per_shard>>> tables;
    std::vector<foreign_ptr<std::unique_ptr<schema_diff_per_shard>>> views;
    std::vector<bool> columns_changed;

    replica::tables_metadata_lock_on_all_shards locks;
    std::unordered_map<table_id, replica::global_table_ptr> table_shards;

    locator::mutable_token_metadata_ptr new_token_metadata; // represents token metadata after updating tablets metadata, nullptr if there was no change
};

// We wrap it with pointer because change_batch needs to be constructed and destructed
// on the same shard as it's used for.
using functions_change_batch_all_shards = std::vector<foreign_ptr<std::unique_ptr<cql3::functions::change_batch>>>;

// contains current types with in-progress modifications applied
class in_progress_types_storage_per_shard : public data_dictionary::user_types_storage {
    const data_dictionary::user_types_storage& _stored_user_types;
    std::map<sstring, data_dictionary::user_types_metadata> _in_progress_types;
public:
    in_progress_types_storage_per_shard(const replica::database& db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types);
    virtual const data_dictionary::user_types_metadata& get(const sstring& ks) const override;
};

class in_progress_types_storage {
    // wrapped in foreign_ptr so they can be destroyed on the right shard
    std::vector<foreign_ptr<shared_ptr<in_progress_types_storage_per_shard>>> shards;
public:
    in_progress_types_storage() : shards(smp::count) {}
    future<> init(distributed<replica::database>& sharded_db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types);
    in_progress_types_storage_per_shard& local();
};

// Schema_applier encapsulates intermediate state needed to construct schema objects from
// set of rows read from system tables (see struct schema_state). It does atomic (per shard)
// application of a new schema.
class schema_applier {
    using keyspace_name = sstring;

    sharded<service::storage_proxy>& _proxy;
    sharded<db::system_keyspace>& _sys_ks;
    const bool _reload;

    std::set<sstring> _keyspaces;
    std::unordered_map<keyspace_name, table_selector> _affected_tables;
    locator::tablet_metadata_change_hint _tablet_hint;

    schema_persisted_state _before;
    schema_persisted_state _after;

    in_progress_types_storage _types_storage;

    affected_keyspaces _affected_keyspaces;
    // during commit we move out some content of _affected_keyspaces,
    // we need just names for notify function
    affected_keyspaces_names _affected_keyspaces_names;
    affected_user_types _affected_user_types;
    affected_tables_and_views _affected_tables_and_views;

    functions_change_batch_all_shards _functions_batch; // includes aggregates

    future<schema_persisted_state> get_schema_persisted_state();
public:
    schema_applier(
            sharded<service::storage_proxy>& proxy,
            sharded<db::system_keyspace>& sys_ks,
            bool reload = false)
            : _proxy(proxy), _sys_ks(sys_ks), _reload(reload) {};

    // Gets called before mutations are applied,
    // preferably no work should be done here but subsystem
    // may do some snapshot of 'before' data.
    future<> prepare(std::vector<mutation>& muts);
    // Update is called after mutations are applied, it should create
    // all updates but not yet commit them to a subsystem (i.e. copy on write style).
    // All changes should be visible only to schema_applier object but not to other subsystems.
    future<> update();
    // Makes updates visible. Before calling this function in memory state as observed by other
    // components should not yet change. The function atomically switches current state with
    // new state (the one built in update function).
    future<> commit();
    // Notify is called after commit and allows to trigger code which can't provide
    // atomicity either for legacy reasons or causes side effects to an external system
    // (e.g. informing client's driver).
    future<> notify();

private:
    void commit_on_shard(replica::database& db);
    void commit_tables_and_views();
    future<> finalize_tables_and_views();
};

}

}
