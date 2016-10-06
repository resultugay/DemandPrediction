import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by resultugay on 06-Sep-16.
  */

object monthly extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkCassandra")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)

  var orderDataFrame = cc.sql("select * from dyna2.product_order")
  orderDataFrame = orderDataFrame.groupBy(
    "productid",
    "sellerid",
    "year",
    "month").agg(
    avg("brandindexed").as("brandindexed"),
    avg("disprice").as("disprice"),
    avg("fastsellerindexed").as("fastsellerindexed"),
    avg("finalprice").as("finalprice"),
    sum("numberofviews").as("numberofviews"),
    avg("price").as("price"),
    sum("productcount").as("productcount"),
    avg("scorefive").as("scorefive"),
    avg("scorefour").as("scorefour"),
    avg("scoreone").as("scoreone"),
    avg("scorethree").as("scorethree"),
    avg("scoretwo").as("scoretwo"),
    avg("sellergrade").as("sellergrade"),
    avg("stock").as("stock") ,
    avg("relativeprice").as("relativeprice"))

  val assembler = new VectorAssembler()
    .setInputCols(Array("brandindexed","disprice","fastsellerindexed","finalprice",
      "numberofviews","price", "scorefive","scorefour","scorethree","scoretwo","scoreone","sellergrade","stock","relativeprice"
    ))
    .setOutputCol("indexedFeaturesAseembler")

  val data = assembler.transform(orderDataFrame).select("productcount","indexedFeaturesAseembler")

  val data2 = data.selectExpr("cast(productcount as Double) productcount","indexedFeaturesAseembler").select("productcount","indexedFeaturesAseembler")

  //decision tree regression
/*
      val featureIndexer = new VectorIndexer()
        .setInputCol("indexedFeaturesAseembler")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(20)
        .fit(data2)

      val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))

      val dt = new DecisionTreeRegressor()
        .setLabelCol("productcount")
        .setFeaturesCol("indexedFeatures")


      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, dt))

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      predictions.show(predictions.count().toInt)


  val max1 = predictions.select(max("prediction")).head().getDouble(0)
  val min1 = predictions.select(min("prediction")).head().getDouble(0)

  // Select (prediction, true label) and compute test error
  val evaluator = new RegressionEvaluator()
    .setLabelCol("productcount")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse/(max1-min1))

      val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
      println("Learned regression tree model:\n" + treeModel.toDebugString)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse/(max1-min1))

*/

  //randomforestregression

  val featureIndexer = new VectorIndexer()
    .setInputCol("indexedFeaturesAseembler")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(20)
    .fit(data2)

  val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))
  val rf = new RandomForestRegressor()
    .setLabelCol("productcount")
    .setFeaturesCol("indexedFeatures")

  val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, rf))

  // Train model.  This also runs the indexer.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions = model.transform(testData)

  // Select example rows to display.
  predictions.select("prediction", "productcount", "indexedFeaturesAseembler").show(100)

    val max1 = predictions.select(max("prediction")).head().getDouble(0)
    val min1 = predictions.select(min("prediction")).head().getDouble(0)

  // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("productcount")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse/(max1-min1))

  val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
  println("Learned regression forest model:\n" + rfModel.toDebugString)

  println("Root Mean Squared Error (RMSE) on test data = " + rmse/(max1-min1))








}