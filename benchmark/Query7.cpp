//
// Created by Seppe Degryse on 09/02/2025.
//

/*
select ?obId1 ?obId2 ?v1 ?v2
from <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>
from stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ> [range 3s step 1s]
from stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN> [range 3s step 1s]
where {
?p1   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.
?p2   a <http://www.insight-centre.org/citytraffic#ParkingVacancy>.


{?obId1 a ?ob.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataKALKVAERKSVEJ>.
}

{?obId2 a ?ob.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusParkingDataSKOLEBAKKEN>.
}

Filter(?v1 < 1 || ?v2 < 1 )
}
*/

#include <benchmark/benchmark.h>

#include "default_checks.hpp"
#include "data/parking_data.hpp"
using namespace parking_data;

extern "C" {
    #include "query.h"
    #include "file_source.h"
}

namespace
{
    bool check_vehicle_count_prop(const triple_t in)
    {
        return in.predicate == SOSA_OBSERVED_PROPERTY && in.object == VEHICLE_COUNT;
    }

    bool check_has_simple_res(const triple_t in)
    {
        return in.predicate == SOSA_HAS_SIMPLE_RESULT && in.object < 210;
    }

    bool check_smaller(const triple_t in)
    {
        return in.predicate == SOSA_HAS_SIMPLE_RESULT && in.object < 210;
    }

    static void BM_Query7(benchmark::State& state)
    {
        // Extract window parameters from the benchmark state
        uint32_t window_size = state.range(0);
        double prob = 8.f/static_cast<double>(window_size);

        source_t *source_kalk1 = create_file_source("../../benchmark/data/AarhusParkingDataKALKVAERKSVEJ.bin", 1);
        source_t *source_kalk2 = create_file_source("../../benchmark/data/AarhusParkingDataKALKVAERKSVEJ.bin", 1);
        source_t *source_skol1 = create_file_source("../../benchmark/data/AarhusParkingDataSKOLEBAKKEN.bin", 1);
        source_t *source_skol2 = create_file_source("../../benchmark/data/AarhusParkingDataSKOLEBAKKEN.bin", 1);
        sink_t *sink = create_file_sink(nullptr);

        window_params_t wparams_skol1 = {window_size, window_size, source_skol1};
        operator_t window_op_skol1 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_skol1}
        };

        window_params_t wparams_skol2 = {window_size, window_size, source_skol2};
        operator_t window_op_skol2 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_skol2}
        };

        window_params_t wparams_kalk1 = {window_size, window_size, source_kalk1};
        operator_t window_op_kalk1 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_kalk1}
        };

        window_params_t wparams_kalk2 = {window_size, window_size, source_kalk2};
        operator_t window_op_kalk2 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_kalk2}
        };

        // STREAM KALK
        filter_check_t vehicle_count_check[1] = {check_vehicle_count_prop};
        operator_t filter_vehicle_count_kalk = {
            .type = FILTER,
            .left = &window_op_kalk1,
            .right = nullptr,
            .params = {.filter = {.size = 1, vehicle_count_check}}
        };

        filter_check_t has_simple_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res_kalk = {
            .type = FILTER,
            .left = &window_op_kalk2,
            .right = nullptr,
            .params = {.filter = {.size = 1, has_simple_res_check}}
        };

        join_check_t cond[1] = {natural_join};
        operator_t join_stream_kalk = {
            .type = JOIN,
            .left = &filter_vehicle_count_kalk,
            .right = &filter_has_simple_res_kalk,
            .params = {.join = {.size = 1, .checks = cond}}
        };


        // STREAM SKOL
        operator_t filter_vehicle_count_skol = {
            .type = FILTER,
            .left = &window_op_skol1,
            .right = nullptr,
            .params = {.filter = {.size = 1, vehicle_count_check}}
        };

        operator_t filter_has_simple_res_skol = {
            .type = FILTER,
            .left = &window_op_skol2,
            .right = nullptr,
            .params = {.filter = {.size = 1, has_simple_res_check}}
        };

        operator_t join_stream_skol = {
            .type = JOIN,
            .left = &filter_vehicle_count_skol,
            .right = &filter_has_simple_res_skol,
            .params = {.join = {.size = 1, .checks = cond}}
        };

        uint8_t predicates[1] = {SOSA_HAS_SIMPLE_RESULT};
        operator_t select_attr1 = {
            .type = SELECT,
            .left = &join_stream_kalk,
            .right = nullptr,
            .params = {.select = {.width = 1, .size = 1, .colums = predicates}}
        };

        operator_t select_attr2 = {
            .type = SELECT,
            .left = &join_stream_skol,
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
            execute_query(&query, sink);
            reset_file_source(source_kalk1);
            reset_file_source(source_kalk2);
            reset_file_source(source_skol1);
            reset_file_source(source_skol2);
        }


        free_file_source(source_kalk1);
        free_file_source(source_kalk2);
        free_file_source(source_skol1);
        free_file_source(source_skol2);
        free_file_sink(sink);

    }
}

BENCHMARK(BM_Query7)->RangeMultiplier(2)->Range(8, 8<<10);