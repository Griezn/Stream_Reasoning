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
    #include "defs.h"
    #include "query.h"
    #include "file_source.h"
    #include "mutli_source.h"
}

bool natural_join(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}

static void query1(benchmark::State& state)
{
    source_t *source1 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 1, 255);
    source_t *source2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 1, 255);
    source_t *msource = create_multi_source();
    sink_t *msink = create_multi_sink();

    multi_source_add(msource, source1);
    multi_source_add(msource, source2);

    uint8_t obs_prop[1] = {SOSA_OBSERVED_PROPERTY};
    operator_t select_obs_prop = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = obs_prop}}
    };

    uint8_t has_value[1] = {SOSA_HAS_SIMPLE_RESULT};
    operator_t select_has_value = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = has_value}}
    };

    uint8_t obs_id[1] = {SOSA_MADE_BY_SENSOR};
    operator_t select_obs_id = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = obs_id}}
    };

    join_check_t cond[1] = {natural_join};
    operator_t join1 = {
        .type = JOIN,
        .left = &select_obs_prop,
        .right = &select_has_value,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    operator_t join2 = {
        .type = JOIN,
        .left = &join1,
        .right = &select_obs_id,
        .params = {.join = {.size = 1, .checks = cond}}
    };

    query_t query = {.root = &join2};

    for (auto _ : state) {
        execute_query(&query, msource, msink);
    }

    free_multi_sink(msink);
    free_multi_source(msource);
    free_file_source(source1);
    free_file_source(source2);
}

BENCHMARK(query1);
