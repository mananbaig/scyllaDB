/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <exception>
#include <optional>
#include <variant>

#include "seastar/core/abort_source.hh"
#include "seastar/core/sstring.hh"
#include "seastar/core/timed_out_error.hh"

#include "utils/exception_container.hh"
#include "utils/result.hh"

namespace replica {

// A marker indicating that the exception_variant holds an unknown exception.
// For example, replica sends a new type of error and coordinator does not
// understand it because it wasn't upgraded to a newer version yet.
struct unknown_exception {};

// A marker indicating that the exception variant doesn't hold any exception.
struct no_exception {};

class replica_exception : public std::exception {
public:
    replica_exception() noexcept {};
};

class rate_limit_exception final : public replica_exception {
public:
    rate_limit_exception() noexcept
            : replica_exception()
    { }

    virtual const char* what() const noexcept override { return "rate limit exceeded"; }
};

using abort_requested_exception = seastar::abort_requested_exception;

struct exception_variant {
    std::variant<unknown_exception,
            no_exception,
            rate_limit_exception,
            abort_requested_exception
    > reason;

    exception_variant()
            : reason(no_exception{})
    { }

    template<typename Ex>
    exception_variant(Ex&& ex)
            : reason(std::move(ex))
    { }

    std::exception_ptr into_exception_ptr() noexcept;

    inline operator bool() const noexcept {
        return !std::holds_alternative<no_exception>(reason);
    }
};

// Tries to encode the exception into an exception_variant.
// If given exception cannot be encoded into one of the replica exception types,
// returns no_exception.
exception_variant try_encode_replica_exception(std::exception_ptr eptr);

}
