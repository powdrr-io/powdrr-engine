import sys

import duckdb


def prepare_duck() -> None:
    duckdb.sql('''
INSTALL iceberg;
LOAD iceberg;
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='admin' ;
SET s3_secret_access_key='password';
SET s3_use_ssl = false;
    ''')


def queries(manifest_path: str) -> None:
    print(duckdb.sql("SELECT count(*) FROM iceberg_scan('s3://{}');".format(manifest_path)))

    print(duckdb.sql("SELECT * FROM iceberg_scan('s3://{}') limit 10;".format(manifest_path)))

    print(duckdb.sql("SELECT orgId, actor_id FROM iceberg_scan('s3://{}') limit 10;".format(manifest_path)))



def do_it(manifest_path: str) -> None:
    prepare_duck()
    queries(manifest_path)


if __name__ == "__main__":
    do_it(sys.argv[1])
