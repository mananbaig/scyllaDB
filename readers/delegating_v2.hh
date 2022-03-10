/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "readers/flat_mutation_reader_v2.hh"

class delegating_reader_v2 : public flat_mutation_reader_v2::impl {
    flat_mutation_reader_v2_opt _underlying_holder;
    flat_mutation_reader_v2* _underlying;
public:
    // when passed a lvalue reference to the reader
    // we don't own it and the caller is responsible
    // for evenetually closing the reader.
    delegating_reader_v2(flat_mutation_reader_v2& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder()
        , _underlying(&r)
    { }
    // when passed a rvalue reference to the reader
    // we assume ownership of it and will close it
    // in close().
    delegating_reader_v2(flat_mutation_reader_v2&& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder(std::move(r))
        , _underlying(&*_underlying_holder)
    { }
    virtual future<> fill_buffer() override {
        if (is_buffer_full()) {
            return make_ready_future<>();
        }
        return _underlying->fill_buffer().then([this] {
            _end_of_stream = _underlying->is_end_of_stream();
            _underlying->move_buffer_content_to(*this);
        });
    }
    virtual future<> fast_forward_to(position_range pr) override {
        _end_of_stream = false;
        forward_buffer_to(pr.start());
        return _underlying->fast_forward_to(std::move(pr));
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        auto maybe_next_partition = make_ready_future<>();
        if (is_buffer_empty()) {
            maybe_next_partition = _underlying->next_partition();
        }
      return maybe_next_partition.then([this] {
        _end_of_stream = _underlying->is_end_of_stream() && _underlying->is_buffer_empty();
      });
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(pr);
    }
    virtual future<> close() noexcept override {
        return _underlying_holder ? _underlying_holder->close() : make_ready_future<>();
    }
};
flat_mutation_reader_v2 make_delegating_reader_v2(flat_mutation_reader_v2&);


