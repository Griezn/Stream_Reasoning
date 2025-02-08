//
// Created by Seppe Degryse on 06/11/2024.
//
#include <benchmark/benchmark.h>


extern "C" {
    #include "generator.h"
    #include "query.h"
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
    query_t query;
    // Create generator source and sink
    source_t *source = create_generator_source(3);
    sink_t *sink = create_generator_sink();

    window_params_t wparams = {36, 36, source};
    operator_t window_op = {
        .type = WINDOW,
        .left = nullptr,
        .right = nullptr,
        .params = {.window = wparams}
    };

    // Setup the query
    filter_check_t conditions1[1] = {check_filter};
    filter_check_t conditions2[1] = {check_filter3};
    join_check_t conditions3[1] = {check_join};
    filter_check_t conditions4[1] = {check_filter4};
    join_check_t conditions5[1] = {check_join2};
    filter_check_t conditions6[1] = {check_filter5};

    operator_t filter_has_skill = {
        .type = FILTER,
        .left = &window_op,
        .right = nullptr,
        .params = {.filter = {.size = 1, .checks = conditions1}}
    };

    operator_t filter_req_skill = {
        .type = FILTER,
        .left = &window_op,
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
        .left = &window_op,
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

    query = {.root = &filter_older};

    // Benchmark loop
    for (auto _ : state) {
        execute_query(&query, sink);
    }

    // Clean up
    free_generator_source(source);
    free_generator_sink(sink);
}

//BENCHMARK(BM_ExecuteQuery);

BENCHMARK_MAIN();
