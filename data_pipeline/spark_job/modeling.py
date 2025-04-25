from data_pipeline.data_cleaning import get_warehouse_model, model_table
from data_pipeline.data_sinking import write_to_postgres
from data_pipeline.utils import get_spark_session

from config.settings import envi   

def main():
    """
    Main function to run the data modeling tasks.
    """

    YAML_FILE = envi.WAREHOUSE_MODEL_YAML
    DATABASE = envi.DATABASE

    # Initialize Spark session
    spark = get_spark_session(app_name="Modeling Data")

    warehouse_model = get_warehouse_model(YAML_FILE)

    dfs = dict()
    for schema in warehouse_model:
        tables = warehouse_model[schema]
        for table in tables:
            meta = tables[table]
            df = spark.sql(f"SELECT * FROM {DATABASE}.{meta.source}")
            dfs[''] = model_table(df, meta)
            write_to_postgres(model, f"{schema}.{table}")

if __name__ == "__main__":
    main()

