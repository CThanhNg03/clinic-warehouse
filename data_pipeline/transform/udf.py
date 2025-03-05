from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

extract_display_udf = udf(lambda x: ",".join([v['display'] for v in x['coding']]) if x else None, StringType())

extract_type_udf = udf(lambda x: x.split("/")[-1] if x else None, StringType())

extract_id_udf = udf(lambda x: x.split(":")[-1] if x is not None else None, StringType())