import fastavro
import json
import argparse


if __name__ == '__main__':
    # create parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema-file', type=str, required=True)
    parser.add_argument('--avro-file', type=str, required=True)

    args = parser.parse_args()

    with open(args.schema_file, 'r') as schema_file:
        schema_dict = json.load(schema_file)

    jsonl_file_name = args.avro_file.replace(".avro", ".jsonl")

    with open(args.avro_file, 'rb') as avro_file, open(jsonl_file_name, 'w') as jsonl_file:
        avro_reader = fastavro.reader(avro_file)
        for record in avro_reader:
            jsonl_file.write(json.dumps(record))
