/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <cstdint>

namespace gms {

using generation_type = int32_t;
using version_type = int32_t;

/**
 * A unique version number generator for any state that is generated by the
 * local node.
 */

namespace version_generator
{
    version_type get_next_version() noexcept;
}

} // namespace gms
