import csv
import struct
from datetime import datetime

predicates = {
    "RDF_TYPE": 0,
    "SOSA_OBSERVED_PROPERTY": 1,
    "SOSA_HAS_SIMPLE_RESULT": 2,
    "SOSA_MADE_BY_SENSOR": 3,
    "SOSA_RESULT_TIME": 4
}

classes = {
    "SOSA_OBSERVATION": 0
}

properties = {
    "AVG_SPEED": 0,
    "VEHICLE_COUNT": 1
}

def convert_to_timestamp(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    return int(dt.timestamp())

triple_struct_format = "III"

filename = "AarhusTrafficData182955"
input_file = f"{filename}.csv"
output_file = f"{filename}.bin"

unique_id = 0

with open(input_file, mode="r") as infile, open(output_file, mode="wb") as outfile:
    reader = csv.DictReader(infile)

    for row in reader:
        obs_id = int(row["_id"])
        avg_speed = int(row["avgSpeed"])
        timestamp = convert_to_timestamp(row["TIMESTAMP"])
        vehicle_count = int(row["vehicleCount"])
        report_id = int(row["REPORT_ID"])

        # First observation (Avg Speed)
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["RDF_TYPE"], classes["SOSA_OBSERVATION"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_OBSERVED_PROPERTY"], properties["AVG_SPEED"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_HAS_SIMPLE_RESULT"], avg_speed))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_MADE_BY_SENSOR"], report_id))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_RESULT_TIME"], timestamp))
        unique_id += 1

        # Second observation (Vehicle Count)
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["RDF_TYPE"], classes["SOSA_OBSERVATION"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_OBSERVED_PROPERTY"], properties["VEHICLE_COUNT"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_HAS_SIMPLE_RESULT"], vehicle_count))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_MADE_BY_SENSOR"], report_id))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_RESULT_TIME"], timestamp))
        unique_id += 1

print(f"Data has been successfully written to {output_file}.")