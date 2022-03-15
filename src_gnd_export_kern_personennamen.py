# exported originally created for biofid project
# https://subversion.hucompute.org/main/team/hiwis/baumartz/gnd/


import json
import gzip
from tqdm import tqdm


def export(input_filename, output_filename):
    # map names to full_names
    set_names = set()
    with open(input_filename, "r", encoding="UTF-8") as input_file:
        line_count = sum([1 for _ in input_file])
        input_file.seek(0)
        for line in tqdm(input_file, total=line_count, desc="loading names"):
            line = line.strip()
            if line.startswith("//"):
                continue
            fields = line.split("#")

            try:
                # full name
                full_name = fields[0].split("(")[0].strip()
                set_names.add(full_name)
                # get other version from meta, if available
                for meta in fields[0].split("(")[1][:-1].split(";"):
                    if meta.startswith("suffix="):
                        name_suf = meta.replace("suffix=", "")
                        set_names.add(name_suf)
                    if meta.startswith("suffix2="):
                        name_suf = meta.replace("suffix2=", "")
                        set_names.add(name_suf)
                    if meta.startswith("x="):
                        name_suf = meta.replace("x=", "")
                        set_names.add(name_suf)
                    if meta.startswith("y="):
                        name_suf = meta.replace("y=", "")
                        set_names.add(name_suf)

            except Exception as ex:
                print(ex)

    # export map
    print(len(set_names))
    with gzip.open(output_filename, "wt", encoding="UTF-8") as output_file:
        for name in sorted(list(set_names)):
            output_file.write(f"{name}\n")


if __name__ == '__main__':
    # input_filename = "/home/daniel/data/hiwi/gnd/Kern-Personennamen_sample100000.lex"
    input_filename = "/home/bagci/data/Kern-Toponyme-List-of-Worldcities.lex"
    # output_filename = "data/Kern-Personennamen_sample100000.lex.json.txt"
    output_filename = "/home/bagci/data/Toponymelist.csv_json.gz"
    export(input_filename, output_filename)
