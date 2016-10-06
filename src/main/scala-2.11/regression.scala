/**
  * Created by resultugay on 01-Sep-16.
  */


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}




object regression extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkCassandra")
    //set Cassandra host address as your local address
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)

  var productDataFrame = cc.sql("select * from dyna.product_order")

  //productDataFrame.printSchema()

  //tüm null olan değerleri NULL ile değiştiriyorum.
  productDataFrame = productDataFrame.na.replace(Array("channel","attrs","brand","fastseller"),Map(""->"NULL"))
  productDataFrame = productDataFrame.na.replace(Array("productid","sellerid"),Map(""->"-999"))
  /*
  productDataFrame = productDataFrame.na.replace(Array("year","month","week","day","disprice",
    "eventcount","finalprice","numberofviews","price","productcount","scorefive",
    "scorefour","scorethree","scoretwo","scoreone","stock"),Map(""-> "-999.0"))
  */
  //Tüm null olan string değerler NULL ile numeric değerler ise -999.0 ile değiştiriliyor.
  productDataFrame = productDataFrame.na.fill(-999.0)
  productDataFrame = productDataFrame.na.fill("NULL")
  /*
  val splits = productDataFrame.rdd.randomSplit(Array(0.4,0.6), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  val  labelled = productDataFrame.map(row => (row(21),
    (row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11),row(12),row(13),row(14),
      row(15),row(17),row(18),row(19),row(20))))


  val trainingData1 = training.map { row =>
  LabeledPoint(0.0, Vectors.dense(Array(0.0,1.0,2.4)))
  }
*/


  // Automatically identify categorical features, and index them.
/*  val featureIndexer = new VectorIndexer()
    .setInputCol("channel")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
    .fit(productDataFrame)
  */

  //Categoric değerler Indexleniyor. Bu aşamada.
  productDataFrame = new StringIndexer().setInputCol("channel").setOutputCol("channelIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("year").setOutputCol("yearIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("month").setOutputCol("monthIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("week").setOutputCol("weekIndexed").fit(productDataFrame).transform(productDataFrame)
  //productDataFrame = new StringIndexer().setInputCol("attrs").setOutputCol("attrsIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("brand").setOutputCol("brandIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("fastseller").setOutputCol("fastsellerIndexed").fit(productDataFrame).transform(productDataFrame)
  productDataFrame = new StringIndexer().setInputCol("stock").setOutputCol("stockIndexed").fit(productDataFrame).transform(productDataFrame)

  //Yeni Dataframemimizden istediğimiz kolonları çıkarıyoruz.
 // productDataFrame.show(200)
  val newOrderDf = productDataFrame.select("stock","sellerid","productid","disprice","eventcount","finalprice","numberofviews","price",
  "productcount","scorefive","scorefour","scorethree","scoretwo","scoreone","stockIndexed","channelIndexed",
  "yearIndexed"/*,"monthIndexed","weekIndexed","brandIndexed"*/,"fastsellerIndexed")

  //newOrderDf.show()
 // newOrderDf.printSchema()

  //Bu arkadaş verilen kolonları vector haline getiriyor.
  val assembler = new VectorAssembler()
    .setInputCols(Array("disprice","eventcount","finalprice","numberofviews","price",
      "productcount","scorefive","scorefour","scorethree","scoretwo","scoreone","channelIndexed",
      "yearIndexed"/*,"monthIndexed","weekIndexed","brandIndexed"*/,"fastsellerIndexed"))
    .setOutputCol("indexedFeaturesAseembler")

  val data = assembler.transform(newOrderDf).select("stock","indexedFeaturesAseembler")
  //data.select("stock","indexedFeaturesAseembler").show()
 // data.show(100)
  data.printSchema()

  val data2 = data.selectExpr("cast(stock as Double) stock","indexedFeaturesAseembler").select("stock","indexedFeaturesAseembler")

  data2.printSchema()
  //data2.show(50)

  val featureIndexer = new VectorIndexer()
    .setInputCol("indexedFeaturesAseembler")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data2)

  val Array(trainingData, testData) = data2.randomSplit(Array(0.7, 0.3))
  //trainingData.select("indexedFeaturesAseembler").show(10)


  val dt = new DecisionTreeRegressor()
    .setLabelCol("stock")
    .setFeaturesCol("indexedFeaturesAseembler")
    //.setMaxBins(42)


  val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, dt))



  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)

  predictions.show()
  predictions.foreach(println)

  // Select (prediction, true label) and compute test error
  val evaluator = new RegressionEvaluator()
    .setLabelCol("stock")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
  println("Learned regression tree model:\n" + treeModel.toDebugString)



  /*

  //Yeni hazır olan dataframe ayrılıyor test ve train olarak.
  val Array(trainingData, testData) = newOrderDf.randomSplit(Array(0.7, 0.3))

  // Classifierımız. Burada label predict edilecek kolon features'da feature vectorumuz.
  //MaxBin parametresi bir kolondaki max farklı
  val dt = new DecisionTreeClassifier()
    .setLabelCol("stockIndexed")
    .setFeaturesCol("indexedFeatures")
    .setMaxBins(158)

  // Chain indexers and tree in a Pipeline
  val pipeline = new Pipeline()
    .setStages(Array( assembler, dt))

  // Train model.  This also runs the indexers.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions = model.transform(testData)


  // Select example rows to display.
 // predictions.select("predictedLabel", "stock", "scoreone").show(5)


  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("stockIndexed")
    .setPredictionCol("prediction")
    .setMetricName("precision")

  val accuracy = evaluator.evaluate(predictions)
  println("Test Error = " + (1.0 - accuracy))

  val treeModel = model.stages(1).asInstanceOf[DecisionTreeClassificationModel]
  println("Learned classification tree model:\n" + treeModel.toDebugString)
*/


}