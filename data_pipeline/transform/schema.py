from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType, BooleanType

reason_reference_schema = ArrayType(
    StructType([
        StructField("reference", StringType(), True)
    ])
)

procedure_reason_schema = StructType([
    StructField("reference", StringType(), True)
])

careplan_category_schema = StructType([
    StructField("coding", ArrayType(
        StructType([
            StructField("code", StringType(), True),
            StructField("display", StringType(), True),
            StructField("system", StringType(), True)
        ])
    ), True)
])

encounter_type_schema = ArrayType(StructType([
            StructField("coding", ArrayType(
                StructType([
                    StructField("code", StringType(), True),
                    StructField("display", StringType(), True),
                    StructField("system", StringType(), True)
                ])
            ), True),
            StructField("text", StringType(), True)
        ]))