//
// Created by Seppe Degryse on 13/02/2025.
//
#include <gtest/gtest.h>

#include "../benchmark/data/traffic_data.hpp"
#include "../benchmark/default_checks.hpp"
using namespace traffic_data; // Because it is the only namespace we use

extern "C" {
#include "query.h"
#include "file_source.h"
}

bool check_avg_speed_prop(const triple_t in)
{
    return in.predicate == SOSA_OBSERVED_PROPERTY && in.object == AVG_SPEED;
}

bool check_has_simple_res(const triple_t in)
{
    return in.predicate == SOSA_HAS_SIMPLE_RESULT;
}

TEST(bench_tests, QUERY1)
{
    // Extract window parameters from the benchmark state
        uint32_t window_size = 8192;

        source_t *source11 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 2);
        source_t *source12 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 2);
        source_t *source21 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 2);
        source_t *source22 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 2);

        //sink_t *sink = create_file_sink("../../tests/query1.bin");
        sink_t *sink = create_file_sink(nullptr);

        window_params_t wparams11 = {window_size, window_size, source11};
        operator_t window_op11 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams11}
        };

        window_params_t wparams12 = {window_size, window_size, source12};
        operator_t window_op12 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams12}
        };

        window_params_t wparams21 = {window_size, window_size, source21};
        operator_t window_op21 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams21}
        };

        window_params_t wparams22 = {window_size, window_size, source22};
        operator_t window_op22 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams22}
        };

        // STREAM 1
        filter_check_t avg_speed_check[1] = {check_avg_speed_prop};
        operator_t filter_avg_speed1 = {
            .type = FILTER,
            .left = &window_op11,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = avg_speed_check}}
        };

        filter_check_t has_simple_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res1 = {
            .type = FILTER,
            .left = &window_op12,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = has_simple_res_check}}
        };

        join_check_t cond[1] = {natural_join};
        operator_t join_stream1 = {
            .type = JOIN,
            .left = &filter_avg_speed1,
            .right = &filter_has_simple_res1,
            .params = {.join = {.size = 1, .checks = cond}}
        };

        // STREAM 1
        operator_t filter_avg_speed2 = {
            .type = FILTER,
            .left = &window_op21,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = avg_speed_check}}
        };

        operator_t filter_has_simple_res2 = {
            .type = FILTER,
            .left = &window_op22,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = has_simple_res_check}}
        };

        operator_t join_stream2 = {
            .type = JOIN,
            .left = &filter_avg_speed2,
            .right = &filter_has_simple_res2,
            .params = {.join = {.size = 1, .checks = cond}}
        };

        uint8_t predicates[1] = {SOSA_HAS_SIMPLE_RESULT};
        operator_t select_attr1 = {
            .type = SELECT,
            .left = &join_stream1,
            .right = nullptr,
            .params = {.select = {.width = 1, .size = 1, .colums = predicates}}
        };

        operator_t select_attr2 = {
            .type = SELECT,
            .left = &join_stream2,
            .right = nullptr,
            .params = {.select = {.width = 1, .size = 1, .colums = predicates}}
        };

        operator_t cart_join_main = {
            .type = CARTESIAN,
            .left = &select_attr1,
            .right = &select_attr2,
            .params = {.cart_join = {.probability = 8.f/8192.f}}
        };

        query_t query = {.root = &cart_join_main};

        execute_query(&query, sink);

        free_file_source(source11);
        free_file_source(source12);
        free_file_source(source21);
        free_file_source(source22);
        free_file_sink(sink);
}

