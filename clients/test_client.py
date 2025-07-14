import datetime
import os
import sys
import time

import requests


BATCH_SIZE = 5


CREATE_INDEX = """{
    "settings" : {
        "number_of_shards" : 2,
        "number_of_replicas" : 1
    }
}"""


ITEM_TEMPLATE = """{ "@timestamp": "{timestamp}", "index_col": {index}, "user": { "id": "vlb44hny" }, "message": "Login attempt failed {index}" }"""

QUERY = """{
    "query": {
      "match": {
        "message": {
          "query": "attempt"
        }
      }
    }
}"""


ALTERNATE_QUERY = """{
    "query": {
      "bool": {
        "must": [
            "term" : {
              {"user.id": "vlb44hny"}
            }
        ]
      }
    }
}"""


def _create_batch(base_id: int) -> str:
    values = []
    for offset in range(BATCH_SIZE):
        values.append('{"create":{ "_index": "test_index1" }}')
        values.append(
            ITEM_TEMPLATE.replace(
                "{timestamp}", datetime.datetime.now().isoformat()
            ).replace("{index}", str(base_id + offset))
        )
    values.append("")
    return "\n".join(values)


def main(port: int, set_test_mode: bool, num_inserts: int, processes: int):
    base_index = int(time.time() * 10000000)

    headers = {'Content-type': 'application/json'}

    if set_test_mode:
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

    process_id = 0
    for index in range(processes - 1):
        process_id = os.fork()
        if process_id != 0:
            break
        
    time_before = time.time()
    time_last = time_before
    for num in range(num_inserts):
        body = _create_batch(base_index + num * BATCH_SIZE);
        # print(body)
        bulk_response = requests.post(
            url="http://localhost:{}/_bulk".format(port),
            data=body,
            headers=headers,
        )
        print("Bulk Ingest Response:")
        print(bulk_response.text)
        if bulk_response.status_code != 200:
            raise Exception("Ingest failed: {}", bulk_response.text)

        time_before_search = time.time()
        search_response = requests.post(
            url="http://localhost:{}/test_index1/_search".format(port), data=QUERY, headers=headers
        )
        if search_response.status_code != 200:
            raise Exception("Search failed: {}".format(search_response.text))

        print(search_response.text)

        time_current = time.time()
        print("{}: Search #{} took {}ms".format(process_id, num, int((time_current - time_before_search)*1000)))

        if num % 100 == 9:
            print("Sleeping for 5s")
            time.sleep(5)
        time_last = time.time()

    time_after = time.time()
    print("{}: {} took {}ms".format(process_id, num_inserts, int((time_after - time_before)*1000)))


if __name__ == "__main__":
    main(
        9200,
        sys.argv[1] != "es",
        int(sys.argv[2]), 
        int(sys.argv[3])
    )
