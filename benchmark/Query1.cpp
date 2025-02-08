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

#include <benchmark/benchmark.h>

#include "data/traffic_data.hpp"
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


bool check_natural_join(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}


bool check_join_all(const triple_t in1, const triple_t in2)
{
    return true;
}


static void BM_Query1(benchmark::State& state)
{
    // Extract window parameters from the benchmark state
    uint32_t window_size = state.range(0);
    uint32_t step_size = state.range(1);

    source_t *source1 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 2);
    source_t *source2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 2);
    sink_t *sink = create_file_sink();

    window_params_t wparams = {window_size, step_size, source1};
    operator_t window_op1 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {window_size, step_size, source2};
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

    join_check_t cond[1] = {check_natural_join};
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

    join_check_t cond_main[1] = {check_join_all};
    operator_t join_main = {
        .type = JOIN,
        .left = &join_stream1,
        .right = &join_stream2,
        .params = {.join = {.size = 1, .checks = cond_main}}
    };

    uint8_t predicates[1] = {SOSA_HAS_SIMPLE_RESULT};
    operator_t select_attr = {
        .type = SELECT,
        .left = &join_main,
        .right = nullptr,
        .params = {.select = {.width = 2, .size = 1, .colums = predicates}}
    };

    query_t query = {.root = &select_attr};

    for (auto _ : state) {
        execute_query(&query, sink);
    }

    free_file_source(source1);
    free_file_source(source2);
    free_file_sink(sink);
}

BENCHMARK(BM_Query1)->RangeMultiplier(2)->Ranges({{256, 2048}, {64, 512}});