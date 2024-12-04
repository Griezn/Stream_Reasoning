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

static void query1(benchmark::State& state)
{
    source_t *source1 = create_file_source("../../benchmark/data/AarhusTrafficData182955.bin", 1, 255);
    source_t *source2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 1, 255);
    source_t *msource = create_multi_source();
    sink_t *msink = create_multi_sink();

    multi_source_add(msource, source1);
    multi_source_add(msource, source2);

    filter_check_t obs_prop[1] = {filter_obs_prop};
    operator_t select_obs_prop = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_prop}}
    };

    filter_check_t has_value[1] = {filter_has_value};
    operator_t select_has_value = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = has_value}}
    };

    filter_check_t obs_id[1] = {filter_obs_id};
    operator_t select_obs_id = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = obs_id}}
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
