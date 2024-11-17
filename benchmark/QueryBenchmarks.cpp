//
// Created by Seppe Degryse on 06/11/2024.
//
#include <benchmark/benchmark.h>


#include "traffic_data.hpp"

extern "C" {
    #include "defs.h"
    #include "generator.h"
    #include "query.h"
    #include "file_source.h"
}


bool check_filter(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_SKILL;
}

bool check_filter3(const triple_t in)
{
    return in.predicate == PREDICATE_REQUIRES_SKILL;
}

bool check_join(const triple_t in1, const triple_t in2)
{
    return in1.object == in2.object;
}

bool check_filter4(const triple_t in)
{
    return in.predicate == PREDICATE_HAS_AGE;
}

bool check_join2(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}

bool check_filter5(const triple_t in)
{
    return in.object > 30;
}

static void BM_ExecuteQuery(benchmark::State& state)
{
    // Setup the query
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    filter_check_t conditions4[1] = {check_filter4};
    join_check_t conditions5[1] = {check_join2};
    filter_check_t conditions6[1] = {check_filter5};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions2}}
    };

    operator_t join_skill = {
        .type = JOIN,
        .left = &filter_has_skill,
        .right = &filter_req_skill,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };

    operator_t filter_has_age = {
        .type = FILTER,
        .left = nullptr,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions4}}
    };

    operator_t join_age = {
        .type = JOIN,
        .left = &filter_has_age,
        .right = &join_skill,
        .params = {.join = {.size = 1, .checks = conditions5}}
    };

    operator_t filter_older = {
        .type = FILTER,
        .left = &join_age,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions6}}
    };

    query_t query = {.root = &filter_older};

    // Create generator source and sink
    source_t *gsource = create_generator_source();
    sink_t *gsink = create_generator_sink();

    // Benchmark loop
    for (auto _ : state) {
        execute_query(&query, gsource, gsink);
    }

    // Clean up
    free_generator_source(gsource);
    free_generator_sink(gsink);
}

BENCHMARK(BM_ExecuteQuery);


bool check_join3(const triple_t in1, const triple_t in2)
{
    return in1.subject == in2.subject;
}


static void BM_traffic_select_join(benchmark::State& state)
{
    uint8_t predicates[1] = {SOSA_OBSERVATION};
    uint8_t predicates2[1] = {SOSA_HAS_SIMPLE_RESULT};
    join_check_t conditions3[1] = {check_join3};

    operator_t select_obs = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = predicates}}
    };

    operator_t select_simple_result = {
        .type = SELECT,
        .left = nullptr,
        .right = nullptr,
        .params = {.select = {.size = 1, .colums = predicates2}}
    };

    operator_t join_obs = {
        .type = JOIN,
        .left = &select_obs,
        .right = &select_simple_result,
        .params = {.join = {.size = 1, .checks = conditions3}}
    };


    query_t query = {.root = &join_obs};

    // Create generator source and sink
    source_t *source = create_file_source("../../benchmark/traffic_triples1.bin", 4, 255);
    sink_t *sink = create_file_sink();

    // Benchmark loop
    for (auto _ : state) {
        execute_query(&query, source, sink);
    }

    // Clean up
    free_file_source(source);
    free_file_sink(sink);
}

BENCHMARK(BM_traffic_select_join);

BENCHMARK_MAIN();
