//
// Created by Seppe Degryse on 09/02/2025.
//

#ifndef PARKING_DATA_HPP
#define PARKING_DATA_HPP
#include <cstdint>

namespace parking_data
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

    enum garage_codes {
        NORREPORT,
        BUSGADEHUSET,
        BRUUNS,
        SKOLEBAKKEN,
        SCANDCENTER,
        SALLING,
        MAGASIN,
        KALKVAERKSVEJ
    };

    enum properties {
        VEHICLE_COUNT,
        TOTAL_SPACES
    };
}

#endif //PARKING_DATA_HPP
