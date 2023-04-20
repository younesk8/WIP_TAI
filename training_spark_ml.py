import io
import logging
import sys
import traceback
from datetime import datetime

import urllib3

from minio import Minio
from pyspark import SparkContext
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
import minio


def create_model(iter, reg, netParam):
    # create LinearRegression estimator and set parameters
    lr = LinearRegression(maxIter=iter, regParam=reg, elasticNetParam=netParam)
    return lr


def set_train_and_validation_ds(data, seed):
    return data.randomSplit([0.7, 0.3], seed=seed)


def test_model(spark, model):
    # Methode pour tester le modèle avec un jeu de données test
    testData = spark.read.format("libsvm").load("path/to/test/file")
    predictions = model.transform(testData)
    predictions.show()


def save_model(model):
    # Methode pour sauvegarder le modèle vers un lieu spécifié
    model.save("path_to_save_model")


def main():
    seed = 49
    # create SparkSession

    try:
        spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()
        logging.info('Spark session successfully created')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)  # To see traceback of the error.
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        exit(0)
    # read input data
    minio_client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    found = minio_client.bucket_exists("warehouse")
    if not found:
        print("Bucket 'donnes-capteurs' n'existe déjà")
    else:
        print("Bucket 'donnes-capteurs' existe déjà")

    obj: urllib3.response.HTTPResponse = minio_client.get_object(
        'warehouse',
        'olist_orders_dataset.csv',
    )

    content = obj.data.decode('utf-8')
    lines = content.splitlines(keepends=True)

    rdd = spark.sparkContext.parallelize(lines)
    df_spark = spark.read.option("header", True).option("inferSchema", True).csv(rdd)
    df_spark.show()


    # Diviser le dataset entre entrainement et validation
    train_data, validation_Data = set_train_and_validation_ds(df_spark, seed)

    train_data.show()
    validation_Data.show()
    lr = create_model(iter=50, reg=0.01, netParam=0.85)

    # fit the model to the data
    model = lr.fit(train_data)

    # print the model coefficients and intercept
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    # save the model

    # evaluate the model on test data


if __name__ == '__main__':
    main()
