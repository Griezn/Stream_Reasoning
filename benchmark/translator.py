import csv
import struct
from datetime import datetime

# Define the mapping of garage codes to numerical values
garage_codes = {
    "BRUUNS": 0,
    "BUSGADEHUSET": 1,
    "KALKVAERKSVEJ": 2,
    "MAGASIN": 3,
    "NORREPORT": 4,
    "SALLING": 5,
    "SCANDCENTER": 6,
    "SKOLEBAKKEN": 7
}

# Define the mapping of attribute names to integer identifiers
attribute_ids = {
    "vehicle_count": 0,
    "update_time": 1,
    "total_spaces": 2,
    "garage_code": 3
}

# Convert a datetime string to a numerical timestamp
def convert_to_timestamp(date_str):
    try:
        # Try parsing with fractional seconds
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        # Fall back to parsing without fractional seconds
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())


# File names
input_filename = "AarhusParkingData.stream"
output_filename = "output_triples.bin"

# Define the struct format string
# 'i' is for integers (4 bytes each): int _id, int attribute_type, int value
triple_struct_format = "iii"

with open(input_filename, mode="r") as infile, open(output_filename, mode="wb") as outfile:
    reader = csv.DictReader(infile)

    for row in reader:
        _id = int(row["_id"])
        vehicle_count = int(row["vehiclecount"])
        update_time = convert_to_timestamp(row["updatetime"])
        total_spaces = int(row["totalspaces"])
        garage_code = garage_codes[row["garagecode"]]
        stream_time = convert_to_timestamp(row["streamtime"])

        # Write each triple separately
        outfile.write(struct.pack(triple_struct_format, _id, attribute_ids["vehicle_count"], vehicle_count))
        outfile.write(struct.pack(triple_struct_format, _id, attribute_ids["update_time"], update_time))
        outfile.write(struct.pack(triple_struct_format, _id, attribute_ids["total_spaces"], total_spaces))
        outfile.write(struct.pack(triple_struct_format, _id, attribute_ids["garage_code"], garage_code))

print(f"Data has been successfully written to {output_filename} in triple format.")
