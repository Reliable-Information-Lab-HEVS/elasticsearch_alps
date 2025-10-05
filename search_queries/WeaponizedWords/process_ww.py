import csv
import json

# filenames
input_file = "/capstor/scratch/cscs/inesaltemir/scripts/search_WORDS/WeaponizedWords/weaponized_ita.csv"   # this file contains JSON text
output_file = "/capstor/scratch/cscs/inesaltemir/scripts/search_WORDS/WeaponizedWords/ww_ita.csv"

# read JSON string from the CSV file
with open(input_file, "r", encoding="utf-8") as f:
    content = f.read().strip()

# parse the JSON
data = json.loads(content)

# write terms to output CSV (no headers)
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    for item in data.get("result", []):
        writer.writerow([item["term"]])
