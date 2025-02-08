//
// Created by Seppe Degryse on 12/11/2024.
//

#ifndef TRAFFIC_DATA_HPP
#define TRAFFIC_DATA_HPP

namespace traffic_data
{
    enum predicates {
        RDF_TYPE,
        SOSA_OBSERVED_PROPERTY,
        SOSA_HAS_SIMPLE_RESULT,
        SOSA_MADE_BY_SENSOR,
        SOSA_RESULT_TIME
    };

    enum categories {
        SOSA_OBSERVATION
    };

    enum properties {
        AVG_SPEED,
        VEHICLE_COUNT
    };
};

#endif //TRAFFIC_DATA_HPP
