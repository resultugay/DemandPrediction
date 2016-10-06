import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by resultugay on 06-Sep-16.
  */

object weekly extends App {

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
    "month",
    "week").agg(
    avg("brandindexed").as("brandindexed"),
    avg("disprice").as("disprice"),
    avg("finalprice").as("finalprice"),
    sum("numberofviews").as("numberofviews"),
    avg("price").as("price"),
    sum("productcount").as("label"),
    avg("scores").as("scores"),
    avg("relativebrand").as("relativebrand"),
    avg("sellergrade").as("sellergrade"),
    avg("stock").as("stock") ,
    avg("relativeprice").as("relativeprice"))

    //println(orderDataFrame.filter("label < 10").count())
    orderDataFrame = orderDataFrame.filter("label < 20")
  //orderDataFrame = orderDataFrame.withColumn("new",log(Math.E,orderDataFrame("label")))

  val assembler = new VectorAssembler()
    .setInputCols(Array("brandindexed","disprice","finalprice",
      "numberofviews","price","relativebrand","relativeprice","scores","sellergrade","stock"
    ))
    .setOutputCol("features")


  val data = assembler.transform(orderDataFrame).select("label","features")

  var data2 = data.selectExpr("cast(label as Double) label","features").select("label","features")


  //decision tree regression
/*
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(39)
        .fit(data2)

      val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))

      val dt = new DecisionTreeRegressor()
        .setLabelCol("label")
        .setMaxBins(39)
        .setFeaturesCol("indexedFeatures")


      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, dt))

      val model = pipeline.fit(trainingData)

      var predictions = model.transform(testData)

      //predictions = predictions.withColumn("prediction2",exp(predictions("prediction")))
      predictions.show()
      //predictions.show(predictions.count().toInt)


   /* val max1 = predictions.select(max("prediction")).head().getDouble(0)
    val min1 = predictions.select(min("prediction")).head().getDouble(0)*/

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)

    val evaluator2 = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val r2 = evaluator2.evaluate(predictions)
    println("r2 on test data = " + r2)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val sta = predictions.selectExpr("prediction as pDecisionTree")
*/

  //randomforestregression

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(40)
        .fit(data2)

  val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))
      val rf = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures").setMaxBins(39)

      val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, rf))


  val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth,Array(4,8,10))
      //.addGrid(rf.impurity,Array("entropy","gini"))
    .build()

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")


  val cv = new CrossValidator()
    // ml.Pipeline with ml.classification.RandomForestClassifier
    .setEstimator(pipeline)
    // ml.evaluation.MulticlassClassificationEvaluator
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(4)

  val model = cv.fit(data2) // trainingData: DataFrame

  val predictions = model.transform(testData)
  predictions.select("prediction", "label", "features").show(100)

  val rmse = evaluator.evaluate(predictions)
  val evaluator2 = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("r2")
  val r2 = evaluator2.evaluate(predictions)
  println("r2 on test data = " + r2)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  //
  /* val model = pipeline.fit(trainingData)

   // Make predictions.
   val predictions = model.transform(testData)

   // Select example rows to display.
   //predictions.select("prediction", "label", "features").show(100)

/*   val max1 = predictions.select(max("prediction")).head().getDouble(0)
   val min1 = predictions.select(min("prediction")).head().getDouble(0)*/

 // Select (prediction, true label) and compute test error
   val evaluator = new RegressionEvaluator()
     .setLabelCol("label")
     .setPredictionCol("prediction")
     .setMetricName("rmse")
   val rmse = evaluator.evaluate(predictions)
 val evaluator2 = new RegressionEvaluator()
   .setLabelCol("label")
   .setPredictionCol("prediction")
   .setMetricName("r2")
 val r2 = evaluator2.evaluate(predictions)
 println("r2 on test data = " + r2)
 println("Root Mean Squared Error (RMSE) on test data = " + rmse)


*/


  //gradient boosting

/*
  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(40)
    .fit(data2)

  val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))

  // Train a GBT model.
  val gbt = new GBTRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(10)

  // Chain indexer and GBT in a Pipeline
  val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, gbt))

  // Train model.  This also runs the indexer.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions = model.transform(testData)

  // Select example rows to display.
  //predictions.select("prediction", "label", "features").show(5)

 /* val max1 = predictions.select(max("prediction")).head().getDouble(0)
  val min1 = predictions.select(min("prediction")).head().getDouble(0)*/

  // Select (prediction, true label) and compute test error
  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)

  val evaluator2 = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("r2")
  val r2 = evaluator2.evaluate(predictions)
  println("r2 on test data = " + r2)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)
*/



  //Linear regression

/*
    val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)


    val lrModel = lr.fit(trainingData)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
*/

}