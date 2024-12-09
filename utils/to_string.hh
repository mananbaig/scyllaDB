/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/format.h>
#include <fmt/std.h>

template <> struct fmt::formatter<std::strong_ordering> : fmt::formatter<string_view> {
    auto format(std::strong_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::weak_ordering> : fmt::formatter<string_view> {
    auto format(std::weak_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::partial_ordering> : fmt::formatter<string_view> {
    auto format(std::partial_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};
