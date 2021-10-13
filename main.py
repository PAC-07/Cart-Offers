from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import functions as F
from shopping import cart_functions
from util import get_spark


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = get_spark.get_spark_session()

    priceList = [('apple', 35), ('banana', 20), ('melon', 50), ('lime', 15)]
    priceSchema = StructType([
        StructField('item', StringType(), True),
        StructField('price', IntegerType(), True)
    ])
    priceDf = spark.createDataFrame(priceList, priceSchema)
    #priceDf.show()


    offerList = [('melon', 'buy one get one free'), ('lime', 'three for two')]
    offerDf = spark.createDataFrame(offerList, ['item', 'offer'])
    #offerDf.show()

    shoppingCart1 = ["apple", "apple", "banana", "melon", "lime"]
    shoppingCart2 = ['apple', 'apple', 'banana', 'melon', 'lime', 'melon', 'lime']
    shoppingCart3 = ['lime', 'lime', 'apple', 'apple', 'banana', 'melon', 'lime', 'melon', 'lime', 'melon']

    print("total basket value = £{0}".format(cart_functions.processCart(shoppingCart1, priceDf, offerDf)))
    print("total basket value = £{0}".format(cart_functions.processCart(shoppingCart2, priceDf, offerDf)))
    print("total basket value = £{0}".format(cart_functions.processCart(shoppingCart3, priceDf, offerDf)))
