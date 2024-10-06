#pragma once

// Use symmetric_coroutine from boost::coroutine, not asymmetric_coroutine from boost::coroutine2
// symmetric_coroutine meets transaction processing, in which each coroutine can freely yield to another
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <boost/coroutine/all.hpp>
#include "common/config.h"

using coro_call_t = boost::coroutines::symmetric_coroutine<void>::call_type;
using coro_yield_t = boost::coroutines::symmetric_coroutine<void>::yield_type;

struct Coroutine {
    Coroutine() : is_wait_poll_(false) {}

    bool is_wait_poll_;  // where waiting for polling network replies, if true, can leave the yield-able corountine list
    coro_id_t coro_id_;
    coro_call_t func_;   // registered coroutine function
    Coroutine* prev_coro_;
    Coroutine* next_coro_;
};