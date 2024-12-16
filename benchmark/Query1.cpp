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

extern "C" {
    #include "query.h"
    #include "file_source.h"
}

bool natural_join(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}

bool filter_obs_prop(const triple_t in)
{
    return in.predicate == SOSA_OBSERVED_PROPERTY;
}

bool filter_has_value(const triple_t in)
{
    return in.predicate == SOSA_HAS_SIMPLE_RESULT;
}

bool filter_obs_id(const triple_t in)
{
    return in.predicate == SOSA_MADE_BY_SENSOR;
}

bool filter_id(const triple_t in)
{
    return in.predicate == SOSA_OBSERVATION;
}

bool check_join_main1(const triple_t in1, const triple_t in2)
{
    return in1.predicate == SOSA_OBSERVATION && in2.predicate == SOSA_OBSERVATION;
}

static void query1(benchmark::State& state)
{
    source_t *source1 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 4);
    source_t *source2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 4);
    sink_t *sink = create_file_sink();

    window_params_t wparams = {255, 255, source1};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    window_params_t wparams2 = {255, 255, source2};
    operator_t window_op2 = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams2}
    };


    filter_check_t obs_prop[1] = {filter_obs_prop};
    operator_t filter_obs_prop = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_prop}}
    };

    filter_check_t has_value[1] = {filter_has_value};
    operator_t filter_has_value = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = has_value}}
    };

    join_check_t cond[1] = {natural_join};
    operator_t join_val_obs = {
        .type = JOIN,
        .left = &filter_obs_prop,
        .right = &filter_has_value,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    filter_check_t obs_id[1] = {filter_obs_id};
    operator_t filter_obs_id = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_id}}
    };

    filter_check_t _id[1] = {filter_id};
    operator_t filter_id = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = _id}}
    };

    operator_t join_obs_id_id = {
        .type = JOIN,
        .left = &filter_id,
        .right = &filter_obs_id,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    operator_t join_stream1 = {
        .type = JOIN,
        .left = &join_obs_id_id,
        .right = &join_val_obs,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    // STREAM 2
    operator_t filter_obs_prop2 = {
        .type = FILTER,
        .left = &window_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_prop}}
    };

    operator_t filter_has_value2 = {
        .type = FILTER,
        .left = &window_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = has_value}}
    };

    operator_t join_val_obs2 = {
        .type = JOIN,
        .left = &filter_obs_prop2,
        .right = &filter_has_value2,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    operator_t filter_obs_id2 = {
        .type = FILTER,
        .left = &window_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_id}}
    };

    operator_t filter_id2 = {
        .type = FILTER,
        .left = &window_op2,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = _id}}
    };

    operator_t join_obs_id_id2 = {
        .type = JOIN,
        .left = &filter_id2,
        .right = &filter_obs_id2,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    operator_t join_stream2 = {
        .type = JOIN,
        .left = &join_obs_id_id2,
        .right = &join_val_obs2,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    // JOIN THE 2 STREAMS
    join_check_t cond_main[2] = {check_join_main1, natural_join};
    operator_t join_main = {
        .type = JOIN,
        .left = &join_stream1,
        .right = &join_stream2,
        .params = {.join = {.size = 2, .checks = cond_main}}
    };

    query_t query = {.root = &join_main};

    for (auto _ : state) {
        execute_query(&query, sink);
    }

    free_file_source(source1);
    free_file_source(source2);
    free_file_sink(sink);
}

BENCHMARK(query1);
