/**
  * Created by rsltgy on 28.06.2016.
  */

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object housing {

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("local[*]", "dyna", conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load("C:\\Users\\resultugay\\Desktop\\housing.txt")

    data.printSchema()

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    // Train model.  This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + rfModel.toDebugString)















  }

}


/**
  * Created by resultugay on 02-Sep-16.
  */

