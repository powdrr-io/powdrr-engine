from typing import List, Dict, Any, Tuple
import datetime
import os
import sys
import time
import json

import requests


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))



CREATE_INDEX = """{
    "settings" : {
        "number_of_shards" : 2,
        "number_of_replicas" : 1
    }
}"""


ITEM_TEMPLATE = """{ "@timestamp": "{timestamp}", "_id": "{index}", "_version": 1, "_seq_no": 1, "index_col": {index}, "_source": "{\\"user_id\\": \\"the_user\\", \\"message\\": \\"Logout also successful\\"}", "user_id": "{user_id}", "message": "Logout also successful" }"""


ALTERNATE_QUERY = """{
    "query": {
      "bool": {
        "must": [
            { "term" :
              {"user_id": "the_user"}
            }
        ]
      }
    }
}"""



def _create_powdrr_schema() -> Dict[str, Any]:
    return {
        "fields": [
            {"name": "_id", "data_type": "String"},
            {"name": "_version", "data_type": "Integer"},
            {"name": "_seq_no", "data_type": "Integer"},
            {"name": "_source", "data_type": "String"},
            {"name": "message", "data_type": "String"},
            {"name": "user_id", "data_type": "String"},
            {"name": "index_col", "data_type": "Integer"},
        ]
    }


def _create_checkpoint_payload(index: str, files: List[str], sizes: List[int]) -> str:
    checkpoint_dict = {
        "table_name": index,
        "checkpoint_id": "fake_id",
        "iceberg_metadata": None,
        "speedboat_metadata": {
            "files": ["file://{}".format(f) for f in files],
            "sizes": sizes,
            "schemas": [_create_powdrr_schema()],
            "file_schemas": [0 for _ in files]
        },
        "deletes_metadata": None,
        "extension_metadata": None,
        "schema": _create_powdrr_schema()
    }

    return json.dumps(checkpoint_dict, indent=2)


def _create_batch(base_id: int, batch_size: int) -> str:
    values = []
    values.append(
        ITEM_TEMPLATE
            .replace("{timestamp}", datetime.datetime.now().isoformat())
            .replace("{index}", str(base_id))
            .replace("{user_id}", "the_user")
    )
    for offset in range(batch_size - 1):
        values.append(
            ITEM_TEMPLATE
                .replace("{timestamp}", datetime.datetime.now().isoformat())
                .replace("{index}", str(base_id + offset))
                .replace("{user_id}", "fake_user")
        )
    values.append("")
    return "\n".join(values)


def _create_files(base_index: int, num_files: int, num_records_per_file: int) -> Tuple[List[str], List[int]]:
    file_names = []
    sizes = []
    for file_num in range(num_files):
        content = _create_batch(base_index + file_num * num_records_per_file, num_records_per_file)
        sizes.append(len(content))

        file_name = os.path.join(SCRIPT_DIR, "data_{}.json".format(file_num))
        file_names.append(file_name)
        with open(file_name, "w") as fh:
            fh.write(content)

    return (file_names, sizes)


def main(port: int, num_files: int, num_records_per_file, num_processes: int):
    base_index = int(time.time() * 10000000)

    headers = {'Content-type': 'application/json'}

    response = requests.put("http://localhost:{}/_test/v1/_testing_and_processing_mode".format(port))
    if response.status_code != 200:
        raise Exception("Failed to put into test mode")

    response_create_index = requests.put(
        "http://localhost:{}/test_index1".format(port),
        data=CREATE_INDEX,
        headers=headers
    )

    print("Create Index Response:")
    print(response_create_index.text)
    if response_create_index.status_code != 200:
        print("Create index failed")


    files, sizes = _create_files(base_index, num_files, num_records_per_file)
    checkpoint_payload = _create_checkpoint_payload("test_index1", files, sizes)

    response_create_index = requests.post(
        "http://localhost:{}/_test/v1/_add_checkpoint".format(port),
        data=checkpoint_payload,
        headers=headers
    )

    print("Create Checkpoint Response, status = {} :".format(response_create_index.status_code))
    print(response_create_index.text)
    if response_create_index.status_code != 200:
        print("Create Checkpoint failed")

    process_id = 0
    for index in range(num_processes - 1):
        process_id = os.fork()
        if process_id != 0:
            break

    print("****************************************************************************************")
    print("****************************************************************************************")
    print("QUERIES STARTING ***********************************************************************")
    print("****************************************************************************************")
    print("****************************************************************************************")

    num = 1
    while True:
        time_before_search = time.time()
        search_response = requests.post(
            url="http://localhost:{}/test_index1/_search".format(port), data=ALTERNATE_QUERY, headers=headers
        )
        if search_response.status_code != 200:
            raise Exception("Search failed: {}".format(search_response.text))

        print(search_response.text)

        time_current = time.time()
        print("{}: Search #{} took {}ms".format(process_id, num, int((time_current - time_before_search)*1000)))
        num += 1


if __name__ == "__main__":
    main(
        9200,
        int(sys.argv[1]),
        int(sys.argv[2]),
        int(sys.argv[3])
    )

