import argparse
from data_pipeline.data_ingestion import fetch_data_from_kafka, fetch_data_from_local, get_last_batch_id
from data_pipeline.utils import get_spark_session


from typing import Literal
def main(source: Literal['local', 'kafka'] = 'local'):
    spark = get_spark_session(app_name="Extract Data")
    
    if source == 'local':
        last_batch = get_last_batch_id(spark)
        fetch_data_from_local(spark, last_batch)
    elif source == 'kafka':
        fetch_data_from_kafka(spark)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from a source")

    parser.add_argument(
        "--source",
        type=str,
        choices=["local", "kafka"],
        default="local",
        help="Source of data: 'local' or 'kafka'",
    )

    args = parser.parse_args()

    main(source=args.source)