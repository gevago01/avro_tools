import fastavro
import json
import argparse

SAMPLE_SIZE = 1000

if __name__ == '__main__':
    # create parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema-file', type=str, required=True)
    parser.add_argument('--avro-file', type=str, required=True)

    args = parser.parse_args()

    with open(args.schema_file, 'r') as schema_file:
        schema_dict = json.load(schema_file)

    sample_list = []

    with open(args.avro_file, 'rb') as avro_file:
        avro_reader = fastavro.reader(avro_file)
        for record in avro_reader:
            sample_list.append(record)
            if len(sample_list) == SAMPLE_SIZE:
                break

    sample_file_name = args.avro_file.replace(".avro", "-sample.avro")

    with open(sample_file_name, 'wb') as avro_sample:
        fastavro.writer(avro_sample, schema_dict, sample_list)
