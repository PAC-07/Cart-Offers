from pyspark.sql import SparkSession


def get_spark_session(appname="pyspark-application"):
    """
    Create spark session if session doesn't exist
    :param appname:
    :return:
    """
    return (
        SparkSession
            .builder
            .appName(appname)
            .config("spark.sql.shuffle.partitions","1")
            .getOrCreate()
    )