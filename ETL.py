import pyspark

from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, when,format_number
import requests
import json


# Fetch data from the API
url = 'https://api.coincap.io/v2/assets'
data = requests.get(url)

if data.status_code == 200:
    json_data = data.json()    
    json_dataa = json_data['data']
    df = spark.createDataFrame(json_dataa)

 
    dff = df\
        .withColumn('maxSupply', when(col('maxSupply').isNull(), lit(0)).otherwise(col('maxSupply').cast(FloatType())))\
           .withColumn('volumeUsd24Hr',when(col('volumeUsd24Hr').isNull(), lit(0)).otherwise(col('volumeUsd24Hr').cast(FloatType())))\
                .withColumn('priceUsd',when(col('priceUsd').isNull(), lit(0)).otherwise(col('priceUsd').cast(FloatType())))\
                    .withColumn('changePercent24Hr',when(col('changePercent24Hr').isNull(), lit(0)).otherwise(col('changePercent24Hr').cast(FloatType())))\
                        .withColumn('vwap24Hr',when(col('vwap24Hr').isNull(), lit(0)).otherwise(col('vwap24Hr').cast(FloatType())))
    # dff.printSchema()
    
    filter_data=dff.filter(col('maxSupply')>0)
    
    df_type=filter_data\
        .withColumn('maxSupply',format_number(col('maxSupply'),2))\
            .withColumn('volumeUsd24Hr',format_number(col('volumeUsd24Hr'),2))\
                .withColumn('priceUsd',format_number(col('priceUsd'),2))\
                    .withColumn('changePercent24Hr',format_number(col('changePercent24Hr'),2))\
                        .withColumn('vwap24Hr',format_number(col('vwap24Hr'),2))

    # Show the  columns
    # df_type.select('*').show()
    # transformed_df.printSchema()
    transformed_df=df_type.withColumn('new',lit(2024))
    table_new=transformed_df.drop('explorer','changePercent24Hr','vwap24Hr')
    table_new.show()

    
    # df_type.write.partitionBy('symbol')\
    #     .mode('overwrite')\
    #     .csv('')
else:
    print('Error fetching data from API')


