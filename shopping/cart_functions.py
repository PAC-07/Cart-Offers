from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import functions as F
from util import get_spark

spark = get_spark.get_spark_session()

@F.udf(returnType=IntegerType())
def applyOffer(itemCount, price, subtotal, offer):
    if(offer is None):
        return subtotal
    else:
        if offer == 'buy one get one free':
            (d, r) = divmod(itemCount, 2)
            offerPrice = (d * price) + (r * price)
        elif offer == 'three for two':
            (d, r) = divmod(itemCount, 3)
            offerPrice = (d * 2 * price) + (r * price)

        return offerPrice


def processCart(cartList, priceDf, offerDf):
    rddCart = spark.sparkContext.parallelize(cartList)
    row_rddCart = rddCart.map(lambda x: Row(x))
    cartDf = spark.createDataFrame(row_rddCart, ['item'])
    aggCartDf = cartDf.groupBy(cartDf.item).agg(F.count(cartDf.item).alias('itemCount'))

    subtotalDf = aggCartDf \
        .join(priceDf, aggCartDf.item == priceDf.item) \
        .select(aggCartDf.item, aggCartDf.itemCount, priceDf.price,
                (aggCartDf.itemCount * priceDf.price).alias('subtotal'))

    offerSubtotalDf = subtotalDf.join(offerDf, 'item', 'leftouter').select(subtotalDf['*'], offerDf.offer)
    total = offerSubtotalDf \
        .select('item', 'itemCount', 'price', 'subtotal', 'offer',
                applyOffer('itemCount', 'price', 'subtotal', 'offer').alias('newtotal')) \
        .agg(F.sum('newtotal').alias("newtotal")).collect()

    return total[0]['newtotal'] / 100