from pyspark.sql import DataFrame


def to_snake_case(name: str) -> str:
    new_name = "_".join(name.lower().split(" "))
    return new_name


def all_columns_to_snake_case(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, to_snake_case(col_name))
    return df