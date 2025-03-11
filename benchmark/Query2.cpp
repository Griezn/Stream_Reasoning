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
        double prob = 8.f/static_cast<double>(window_size);

        source_t *tsource1 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 1);
        source_t *tsource2 = create_file_source("../../benchmark/data/AarhusTrafficData158505.bin", 1);
        source_t *wsource1 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource2 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource3 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource4 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource5 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource6 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource7 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource8 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        source_t *wsource9 = create_file_source("../../benchmark/data/AarhusWeatherData0.bin", 1);
        sink_t *sink = create_file_sink(nullptr);

        window_params_t wparams_traffic1 = {window_size, window_size, tsource1};
        operator_t window_op_traffic1 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_traffic1}
        };

        window_params_t wparams_traffic2 = {window_size, window_size, tsource2};
        operator_t window_op_traffic2 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_traffic2}
        };

        window_params_t wparams_weather1 = {window_size, window_size, wsource1};
        operator_t window_op_weather1 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather1}
        };

        window_params_t wparams_weather2 = {window_size, window_size, wsource2};
        operator_t window_op_weather2 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather2}
        };

        window_params_t wparams_weather3 = {window_size, window_size, wsource3};
        operator_t window_op_weather3 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather3}
        };

        window_params_t wparams_weather4 = {window_size, window_size, wsource4};
        operator_t window_op_weather4 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather4}
        };

        window_params_t wparams_weather5 = {window_size, window_size, wsource5};
        operator_t window_op_weather5 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather5}
        };

        window_params_t wparams_weather6 = {window_size, window_size, wsource6};
        operator_t window_op_weather6 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather6}
        };

        window_params_t wparams_weather7 = {window_size, window_size, wsource7};
        operator_t window_op_weather7 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather7}
        };

        window_params_t wparams_weather8 = {window_size, window_size, wsource8};
        operator_t window_op_weather8 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather8}
        };

        window_params_t wparams_weather9 = {window_size, window_size, wsource9};
        operator_t window_op_weather9 = {
            .type = WINDOW,
            .left = nullptr,
            .right = nullptr,
            .params = {.window = wparams_weather9}
        };

        // WEATHER DATA
        // TEMPERATURE
        filter_check_t temp_prop_checks[1] = {check_temp_prop};
        operator_t filter_temp_prop {
            .type = FILTER,
            .left = &window_op_weather1,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = temp_prop_checks}}
        };

        filter_check_t has_simp_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res = {
            .type = FILTER,
            .left = &window_op_weather2,
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
            .left = &window_op_weather3,
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
            .left = &window_op_weather4,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = hum_prop_checks}}
        };

        operator_t filter_has_simple_res_hum = {
            .type = FILTER,
            .left = &window_op_weather5,
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
            .left = &window_op_weather6,
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
            .left = &window_op_weather7,
            .right = nullptr,
            .params = {.filter = {.size = 1, .checks = wspd_prop_checks}}
        };

        operator_t filter_has_simple_res_wspd = {
            .type = FILTER,
            .left = &window_op_weather8,
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
            .left = &window_op_weather9,
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

        uint8_t predicates[1] = {weather_data::SOSA_HAS_SIMPLE_RESULT};
        operator_t select_attr1 = {
            .type = SELECT,
            .left = &join_temp_hum_wspd,
            .right = nullptr,
            .params = {.select = {.width = 3, .size = 1, .colums = predicates}}
        };

        // TRAFFIC
        filter_check_t avg_speed_check[1] = {check_avg_speed_prop};
        operator_t filter_avg_speed1 = {
            .type = FILTER,
            .left = &window_op_traffic1,
            .right = nullptr,
            .params = {.filter = {.size = 1, avg_speed_check}}
        };

        filter_check_t has_simple_res_check[1] = {check_has_simple_res};
        operator_t filter_has_simple_res1 = {
            .type = FILTER,
            .left = &window_op_traffic2,
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

        operator_t select_attr2 = {
            .type = SELECT,
            .left = &join_traffic,
            .right = nullptr,
            .params = {.select = {.width = 3, .size = 1, .colums = predicates}}
        };

        // MAIN JOIN
        operator_t cart_join_main = {
            .type = CARTESIAN,
            .left = &select_attr1,
            .right = &select_attr2,
            .params = {.cart_join = {.probability = prob}}
        };

        query_t query = {.root = &cart_join_main};

        for (auto _ : state) {
            execute_query(&query, sink);
            reset_file_source(wsource1);
            reset_file_source(wsource2);
            reset_file_source(wsource3);
            reset_file_source(wsource4);
            reset_file_source(wsource5);
            reset_file_source(wsource6);
            reset_file_source(wsource7);
            reset_file_source(wsource8);
            reset_file_source(wsource9);
            reset_file_source(tsource1);
            reset_file_source(tsource2);
        }


        free_file_source(tsource1);
        free_file_source(tsource2);
        free_file_source(wsource1);
        free_file_source(wsource2);
        free_file_source(wsource3);
        free_file_source(wsource4);
        free_file_source(wsource5);
        free_file_source(wsource6);
        free_file_source(wsource7);
        free_file_source(wsource8);
        free_file_source(wsource9);
        free_file_sink(sink);
    }
}

BENCHMARK(BM_Query2)->RangeMultiplier(2)->Range(8, 8<<10);