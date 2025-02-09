import csv
import struct
from datetime import datetime

predicates = {
    "RDF_TYPE": 0,
    "SOSA_OBSERVED_PROPERTY": 1,
    "SOSA_HAS_SIMPLE_RESULT": 2,
    "SOSA_RESULT_TIME": 4
}

classes = {
    "SOSA_OBSERVATION": 0
}

properties = {
    "HUMIDITY": 0,
    "TEMPERATURE": 1,
    "WIND_SPEED": 2,
}

def convert_to_timestamp(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    return int(dt.timestamp())

triple_struct_format = "III"

filename = "AarhusWeatherData0"
input_file = f"{filename}.csv"
output_file = f"{filename}.bin"

unique_id = 0

with open(input_file, mode="r") as infile, open(output_file, mode="wb") as outfile:
    reader = csv.DictReader(infile)

    for row in reader:
        hum = int(row["hum"])
        temp = round(float(row["tempm"]))  # Round temperature to nearest integer
        wspdm = round(float(row["wspdm"]))  # Round wind speed to nearest integer
        timestamp = convert_to_timestamp(row["TIMESTAMP"])

        # First observation (Humidity)
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["RDF_TYPE"], classes["SOSA_OBSERVATION"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_OBSERVED_PROPERTY"], properties["HUMIDITY"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_HAS_SIMPLE_RESULT"], hum))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_RESULT_TIME"], timestamp))
        unique_id += 1

        # Second  observation (Temperature)
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["RDF_TYPE"], classes["SOSA_OBSERVATION"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_OBSERVED_PROPERTY"], properties["TEMPERATURE"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_HAS_SIMPLE_RESULT"], temp))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_RESULT_TIME"], timestamp))
        unique_id += 1

        # Third observation (Wind Speed)
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["RDF_TYPE"], classes["SOSA_OBSERVATION"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_OBSERVED_PROPERTY"], properties["WIND_SPEED"]))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_HAS_SIMPLE_RESULT"], wspdm))
        outfile.write(struct.pack(triple_struct_format, unique_id, predicates["SOSA_RESULT_TIME"], timestamp))
        unique_id += 1

print(f"Data has been successfully written to {output_file}.")