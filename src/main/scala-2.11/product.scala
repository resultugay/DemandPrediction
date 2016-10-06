import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
/**
  * Created by resultugay on 05-Sep-16.
  */

object product extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkCassandra")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)

  var orderDataFrame = cc.sql("select * from dyna2.product_order_summary")

  var viewDataFrame = cc.sql("select * from dyna2.product_view_summary")

  var groupedViewDataFrame = viewDataFrame.groupBy(
    "productid",
    "brand"
    //"fastseller",
    //"channel", //mobile_web,mobile,web karisiklik yapiyor.
    ).agg(count("merchantid").as("numberOfViews"),
    avg("scorefive").as("scorefive"),
    avg("scorefour").as("scorefour"),
    avg("scorethree").as("scorethree"),
    avg("scoretwo").as("scoretwo"),
    avg("scoreone").as("scoreone"),
    avg("sellergrade").as("sellergrade")
  )

  //groupedViewDataFrame.show(250)
  //println(groupedViewDataFrame.count())

  var productDataFrame = orderDataFrame.join(groupedViewDataFrame,
   ( orderDataFrame(orderDataFrame("productid").toString()) === groupedViewDataFrame(groupedViewDataFrame("productid").toString()))
    ,"left").select(orderDataFrame("productid"),
    orderDataFrame("year"),
    orderDataFrame("month"),
    orderDataFrame("week"),
    orderDataFrame("day"),
    orderDataFrame("attrs"),
    orderDataFrame("channel"),
    orderDataFrame("disprice"),
    orderDataFrame("eventcount"),
    orderDataFrame("finalprice"),
    orderDataFrame("price"),
    orderDataFrame("productcount"),
    orderDataFrame("sellerid"),
    orderDataFrame("stock"),
    //groupedViewDataFrame("fastseller"),
    groupedViewDataFrame("scorefive"),
    groupedViewDataFrame("scorefour"),
    groupedViewDataFrame("scorethree"),
    groupedViewDataFrame("scoretwo"),
    groupedViewDataFrame("scoreone"),
    groupedViewDataFrame("sellergrade"),
    groupedViewDataFrame("numberOfViews"),
    groupedViewDataFrame("brand")
  )

 //11559 instances
  //println(productDataFrame.count())

 // productDataFrame = productDataFrame.na.replace(Array("brand","fastseller"),Map(""->"NULL"))
//  productDataFrame = productDataFrame.na.replace(Array("productid","sellerid"),Map(""->"-999"))

  productDataFrame = productDataFrame.na.fill(-999.0)
  productDataFrame = productDataFrame.na.fill("NULL")


  productDataFrame = new StringIndexer().setInputCol("brand").setOutputCol("brandIndexed").fit(productDataFrame).transform(productDataFrame)


  var newOrderDf = productDataFrame.select("productid","sellerid","year","month","week","day","disprice","brand",
  "finalprice","price", "stock","scorefive","scorefour","scorethree","scoretwo","scoreone","sellergrade","numberOfViews",
  "brandIndexed","productcount")

  //newOrderDf.select("productcount").foreach(println)

  val sqlContext = new SQLContext(sc)

  newOrderDf=newOrderDf.withColumn("scores",newOrderDf("scorefive") *5
    + newOrderDf("scorefour")*4
    + newOrderDf("scorethree")*3
    + newOrderDf("scoretwo")*2
    + newOrderDf("scoreone"))

  newOrderDf = newOrderDf.withColumn("relativePrice",expr("case when price > 3000 then 3.0 when price > 2000 then 2.0" +
    "when price > 1000 then 1.0 else 0 end"))


  newOrderDf = newOrderDf.withColumn("stock",expr("case when stock > 3 then 1.0 else 0 end"))
  //newOrderDf.show()

  val relativeBrand =
    newOrderDf.groupBy("relativePrice","brand").agg(count("productid").as("relativeBrand"))

  newOrderDf = newOrderDf.join(relativeBrand,
    ( newOrderDf(newOrderDf("relativePrice").toString()) === relativeBrand(relativeBrand("relativePrice").toString()) &&
      newOrderDf(newOrderDf("brand").toString()) === relativeBrand(relativeBrand("brand").toString()) ),
    "left"
  ).select(
    newOrderDf("productid"),
    newOrderDf("year"),
    newOrderDf("month"),
    newOrderDf("week"),
    newOrderDf("day"),
    newOrderDf("brand"),
    newOrderDf("brandIndexed"),
    newOrderDf("disprice"),
    newOrderDf("finalprice"),
    newOrderDf("numberOfViews"),
    newOrderDf("price"),
    newOrderDf("productcount"),
    relativeBrand("relativeBrand"),
    newOrderDf("relativePrice"),
    newOrderDf("scores"),
    newOrderDf("sellergrade"),
    newOrderDf("sellerid"),
    newOrderDf("stock")
  )

  //newOrderDf.select("productid","brand","relativePrice","relativeBrand").show(200)


  val a = newOrderDf.rdd.map(x => (
    x.getAs("productid"),
    x.getAs("year"),
    x.getAs("month"),
    x.getAs("week"),
    x.getAs("day"),
    x.getAs("brandIndexed"),
    x.getAs("disprice"),
    x.getAs("finalprice"),
    x.getAs("numberOfViews"),
    x.getAs("price"),
    x.getAs("productcount"),
    x.getAs("relativeBrand"),
    x.getAs("relativePrice"),
    x.getAs("scores"),
    x.getAs("sellergrade"),
    x.getAs("sellerid"),
    x.getAs("stock")
    ) )

    println(a.count())
    a.saveToCassandra("dyna2","product_order")



  /*

  val assembler = new VectorAssembler()
    .setInputCols(Array("channelIndexed","disprice","eventcount","finalprice","price",
      "stock","fastsellerIndexed","scorefive","scorefour","scorethree","scoretwo","scoreone","sellergrade","numberOfViews",
      "brandIndexed"))
    .setOutputCol("indexedFeaturesAseembler")


  val data = assembler.transform(newOrderDf).select("productcount","indexedFeaturesAseembler")

  val data2 = data.selectExpr("cast(productcount as Double) productcount","indexedFeaturesAseembler").select("productcount","indexedFeaturesAseembler")

  println(data2.count())

  */

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
  .setMaxBins(42)


  val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, dt))

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)

  predictions.show(predictions.count().toInt)

  // Select (prediction, true label) and compute test error
  val evaluator = new RegressionEvaluator()
    .setLabelCol("productcount")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
  println("Learned regression tree model:\n" + treeModel.toDebugString)
*/

  //randomforestregression
  /*
  val featureIndexer = new VectorIndexer()
    .setInputCol("indexedFeaturesAseembler")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(20)
    .fit(data2)

  val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))
  val rf = new RandomForestRegressor()
    .setLabelCol("productcount")
    .setFeaturesCol("indexedFeatures")
    .setMaxBins(42)

  val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, rf))

  // Train model.  This also runs the indexer.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions = model.transform(testData)

  // Select example rows to display.
  predictions.select("prediction", "productcount", "indexedFeaturesAseembler").show(100)

  // Select (prediction, true label) and compute test error
  val evaluator = new RegressionEvaluator()
    .setLabelCol("productcount")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
  println("Learned regression forest model:\n" + rfModel.toDebugString)
*/
}