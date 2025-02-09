//
// Created by Seppe Degryse on 12/11/2025.
//

#ifndef WEATHER_DATA_HPP
#define WEATHER_DATA_HPP

namespace weather_data
{
    enum predicates {
        RDF_TYPE,
        SOSA_OBSERVED_PROPERTY,
        SOSA_HAS_SIMPLE_RESULT,
        SOSA_RESULT_TIME
    };

    enum categories {
        SOSA_OBSERVATION
    };

    enum properties {
        HUMIDITY,
        TEMPERATURE,
        WIND_SPEED
    };
};

#endif //WEATHER_DATA_HPP
