import pytest
from tests import conftest
from shopping import cart_functions
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

@pytest.fixture(scope="session")
def spark_test_session():
    return (
        SparkSession
        .builder
        .master('local[*]')
        .appName('unit-testing')
        .getOrCreate()
    )

#spark = conftest.spark_test_session()
#class ShoppingTest:
def test_process_cart(spark_test_session):
    priceList = [('apple', 35), ('banana', 20), ('melon', 50), ('lime', 15)]
    priceSchema = StructType([
        StructField('item', StringType(), True),
        StructField('price', IntegerType(), True)
    ])
    priceDf = spark_test_session.createDataFrame(priceList, priceSchema)
    # priceDf.show()

    offerList = [('melon', 'buy one get one free'), ('lime', 'three for two')]
    offerDf = spark_test_session.createDataFrame(offerList, ['item', 'offer'])

    shoppingCart1 = ["apple", "apple", "banana", "melon", "lime"]
    actual_total = cart_functions.processCart(shoppingCart1, priceDf, offerDf)
    exp_total = 1.55

    assert actual_total == exp_total

def test_process_cart2(spark_test_session):
    priceList = [('apple', 35), ('banana', 20), ('melon', 50), ('lime', 15)]
    priceSchema = StructType([
        StructField('item', StringType(), True),
        StructField('price', IntegerType(), True)
    ])
    priceDf = spark_test_session.createDataFrame(priceList, priceSchema)
    # priceDf.show()

    offerList = [('melon', 'buy one get one free'), ('lime', 'three for two')]
    offerDf = spark_test_session.createDataFrame(offerList, ['item', 'offer'])

    shoppingCart2 = ['apple', 'apple', 'banana', 'melon', 'lime', 'melon', 'lime']
    actual_total = cart_functions.processCart(shoppingCart2, priceDf, offerDf)
    exp_total = 1.70

    assert actual_total == exp_total