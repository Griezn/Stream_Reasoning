//
// Created by Seppe Degryse on 08/02/2025.
//

/*
select ?obId1 ?obId2 ?obId3 ?obId4 ?v1 ?v2 ?v3 ?v4
FROM <http://127.0.0.1:9000/WebGlCity/RDF/SensorRepository.rdf>
FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0> [range 3s step 1s]
FROM stream <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505> [range 3000ms step 1s]

where {

#?p1   a <http://www.insight-centre.org/citytraffic#Temperature>.
#?p2   a <http://www.insight-centre.org/citytraffic#Humidity>.
#?p3   a <http://www.insight-centre.org/citytraffic#WindSpeed>.
#?p4   a <http://www.insight-centre.org/citytraffic#CongestionLevel>.


{
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p1.
?obId1 <http://purl.oclc.org/NET/sao/hasValue> ?v1.
?obId1 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.


?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p2.
?obId2 <http://purl.oclc.org/NET/sao/hasValue> ?v2.
?obId2 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.


?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p3.
?obId3 <http://purl.oclc.org/NET/sao/hasValue> ?v3.
?obId3 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusWeatherData0>.
}

{
?obId4 <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?p4.
?obId4 <http://purl.oclc.org/NET/sao/hasValue> ?v4.
?obId4 <http://purl.oclc.org/NET/ssnx/ssn#observedBy> <http://www.insight-centre.org/dataset/SampleEventService#AarhusTrafficData158505>.
}}
*/

#include <benchmark/benchmark.h>

#include "default_checks.hpp"
#include "data/traffic_data.hpp"
#include "data/weather_data.hpp"

extern "C" {
    #include "query.h"
    #include "file_source.h"
}

namespace
{
    bool check_temp_prop(const triple_t in)
    {
        return in.predicate == weather_data::SOSA_OBSERVED_PROPERTY
            && in.object == weather_data::TEMPERATURE;
    }

    bool check_humidity_prop(const triple_t in)
    {
        return in.predicate == weather_data::SOSA_OBSERVED_PROPERTY
            && in.object == weather_data::HUMIDITY;
    }

    bool check_wspd_prop(const triple_t in)
    {
        return in.predicate == weather_data::SOSA_OBSERVED_PROPERTY
            && in.object == weather_data::WIND_SPEED;
    }

    bool check_has_simple_res(const triple_t in)
    {
        return in.predicate == weather_data::SOSA_HAS_SIMPLE_RESULT;
    }

    bool check_result_time(const triple_t in)
    {
        return in.predicate == weather_data::SOSA_RESULT_TIME;
    }

    bool check_same_timestamp(const triple_t in1, const triple_t in2)
    {
        return (in1.predicate == weather_data::SOSA_RESULT_TIME && in2.predicate == weather_data::SOSA_RESULT_TIME)
                && (in1.object == in2.object);
    }

    bool check_avg_speed_prop(const triple_t in)
    {
        return in.predicate == traffic_data::SOSA_OBSERVED_PROPERTY && in.object == traffic_data::AVG_SPEED;
    }

    static void BM_Query2(benchmark::State& state)
    {
        // Extract window parameters from the benchmark state
        uint32_t window_size = state.range(0);

        source_t *tsource = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 2);
        source_t *wsource = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 9);
        sink_t *sink = create_file_sink(nullptr);

        window_params_t wparams_traffic = {window_size, window_size, tsource};
        operator_t window_op_traffic = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_traffic}
        };

        window_params_t wparams_weather = {window_size, window_size, wsource};
        operator_t window_op_weather = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather}
        };

        // WEATHER DATA
        // TEMPERATURE
        filter_check_t temp_prop_checks[1] = {check_temp_prop};
        operator_t filter_temp_prop {
            .type = FILTER,
            .left = &window_op_traffic,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = temp_prop_checks}}
        };

        filter_check_t has_simp_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = has_simp_res_check}}
        };

        join_check_t natural_join_checks[1] = {natural_join};
        operator_t join_temp = {
            .type = JOIN,
            .left = &filter_temp_prop,
            .right = &filter_has_simple_res,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        filter_check_t result_time_check[1] = {check_result_time};
        operator_t filter_result_time = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = result_time_check}}
        };

        operator_t join_temp_time = {
            .type = JOIN,
            .left = &join_temp,
            .right = &filter_result_time,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        // HUM
        filter_check_t hum_prop_checks[1] = {check_humidity_prop};
        operator_t filter_hum_prop {
            .type = FILTER,
            .left = &window_op_traffic,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = hum_prop_checks}}
        };

        operator_t filter_has_simple_res_hum = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = has_simp_res_check}}
        };

        operator_t join_hum = {
            .type = JOIN,
            .left = &filter_hum_prop,
            .right = &filter_has_simple_res_hum,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        operator_t filter_result_time_hum = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = result_time_check}}
        };

        operator_t join_hum_time = {
            .type = JOIN,
            .left = &join_hum,
            .right = &filter_result_time_hum,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        // HUM
        filter_check_t wspd_prop_checks[1] = {check_wspd_prop};
        operator_t filter_wspd_prop {
            .type = FILTER,
            .left = &window_op_traffic,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = wspd_prop_checks}}
        };

        operator_t filter_has_simple_res_wspd = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = has_simp_res_check}}
        };

        operator_t join_wspd = {
            .type = JOIN,
            .left = &filter_wspd_prop,
            .right = &filter_has_simple_res_wspd,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        operator_t filter_result_time_wspd = {
            .type = FILTER,
            .left = &window_op_weather,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = result_time_check}}
        };

        operator_t join_wspd_time = {
            .type = JOIN,
            .left = &join_wspd,
            .right = &filter_result_time_wspd,
            .params = {.join = {.size = 1, .checks = natural_join_checks}}
        };

        join_check_t eq_time_check[1] = {check_same_timestamp};
        operator_t join_temp_hum = {
            .type = JOIN,
            .left = &join_temp_time,
            .right = &join_hum_time,
            .params = {.join = {.size = 1, .checks = eq_time_check}}
        };

        operator_t join_temp_hum_wspd = {
            .type = JOIN,
            .left = &join_temp_hum,
            .right = &join_wspd_time,
            .params = {.join = {.size = 1, .checks = eq_time_check}}
        };

        // TRAFFIC
        filter_check_t avg_speed_check[1] = {check_avg_speed_prop};
        operator_t filter_avg_speed1 = {
            .type = FILTER,
            .left = &window_op_traffic,
            .right = nullptr,
            .params = {.filter = {.size = 1, avg_speed_check}}
        };

        filter_check_t has_simple_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res1 = {
            .type = FILTER,
            .left = &window_op_traffic,
            .right = nullptr,
            .params = {.filter = {.size = 1, has_simple_res_check}}
        };

        join_check_t cond[1] = {natural_join};
        operator_t join_traffic = {
            .type = JOIN,
            .left = &filter_avg_speed1,
            .right = &filter_has_simple_res1,
            .params = {.join = {.size = 1, .checks = cond}}
        };

        // MAIN JOIN
        join_check_t cond_main[1] = {cartesian_join};
        operator_t join_main = {
            .type = JOIN,
            .left = &join_temp_hum_wspd,
            .right = &join_traffic,
            .params = {.join = {.size = 1, .checks = cond_main}}
        };

        query_t query = {.root = &join_main};

        for (auto _ : state) {
            execute_query(&query, sink);
        }

        free_file_source(tsource);
        free_file_source(wsource);
        free_file_sink(sink);
    }
}

BENCHMARK(BM_Query2)->RangeMultiplier(2)->Range(8, 8<<10);