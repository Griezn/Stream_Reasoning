//
// Created by Seppe Degryse on 01/12/2024.
//

/*
select ?obId1 ?obId2 ?v1 ?v2
from stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955> [range 3000ms step 1s]
from stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3000ms step 1s]
FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>

where {

?p1   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.
?p2   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.


{
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData182955>.
}

{
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}}
*/

#include <iostream>
#include <benchmark/benchmark.h>

#include "data/traffic_data.hpp"
#include "default_checks.hpp"
using namespace traffic_data; // Because it is the only namespace we use

extern "C" {
    #include "query.h"
    #include "file_source.h"
    #include "memory.h"
}

namespace
{
    bool check_avg_speed_prop(const triple_t in)
    {
        return in.predicate == SOSA_OBSERVED_PROPERTY && in.object == AVG_SPEED;
    }

    bool check_has_simple_res(const triple_t in)
    {
        return in.predicate == SOSA_HAS_SIMPLE_RESULT;
    }

    static void BM_Query1(benchmark::State& state)
    {
        // Extract window parameters from the benchmark state
        uint32_t window_size = state.range(0);
        double prob = 8.f/static_cast<double>(window_size);

        source_t *source1 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 2);
        source_t *source2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 2);
        sink_t *sink = create_file_sink(nullptr);

        window_params_t wparams = {window_size, window_size, source1};
        operator_t window_op1 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams}
        };

        window_params_t wparams2 = {window_size, window_size, source2};
        operator_t window_op2 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams2}
        };

        // STREAM 1
        filter_check_t avg_speed_check[1] = {check_avg_speed_prop};
        operator_t filter_avg_speed1 = {
            .type = FILTER,
            .left = &window_op1,
            .right = nullptr,
            .params = {.filter = {.size = 1, avg_speed_check}}
        };

        filter_check_t has_simple_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res1 = {
            .type = FILTER,
            .left = &window_op1,
            .right = nullptr,
            .params = {.filter = {.size = 1, has_simple_res_check}}
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
            .left = &window_op2,
            .right = nullptr,
            .params = {.filter = {.size = 1, avg_speed_check}}
        };

        operator_t filter_has_simple_res2 = {
            .type = FILTER,
            .left = &window_op2,
            .right = nullptr,
            .params = {.filter = {.size = 1, has_simple_res_check}}
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
            .params = {.cart_join = {.probability = prob}}
        };

        query_t query = {.root = &cart_join_main};

        for (auto _ : state) {
            reset_memory_counter();
            execute_query(&query, sink);
            reset_file_source(source1);
            reset_file_source(source2);
        }

        state.counters["Allocs"] = (double) get_alloc_count();
        state.counters["Total"] = (double) get_total_allocated();

        free_file_source(source1);
        free_file_source(source2);
        free_file_sink(sink);
    }
}

BENCHMARK(BM_Query1)->RangeMultiplier(2)->Range(8, 8<<10);