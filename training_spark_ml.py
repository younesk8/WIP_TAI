from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

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
    spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

    # read input data
    data = spark.read.format("libsvm").load("path/to/input/file")
    
    # Diviser le dataset entre entrainement et validation
    train_data, validation_Data = set_train_and_validation_ds(data, seed)

    lr = create_model()

    # fit the model to the data
    model = lr.fit(train_data)

    # print the model coefficients and intercept
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    # save the model

    # evaluate the model on test data
    

if __name__ :
    main()