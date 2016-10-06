/**
  * Created by resultugay on 01-Sep-16.
  */

/**
  * Created by resultugay on 29-Aug-16.
  */

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


object test extends App {

  //standart cassandra ayarlari
  //cassandra 127.0.0.1 ip ve default 9042 portunda kosuyor.
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkCassandra")
    //set Cassandra host address as your local address
    .set("spark.cassandra.connection.host", "127.0.0.1")
  //sparkContext baslatiliyor.
  val sc = new SparkContext(conf)

  //null olan attrs kayitlarindan dolayi hata veriyordu.
  /*val order = sc.cassandraTable[order]("dyna","product_view_summary").cache()
  val view = sc.cassandraTable[view]("dyna","product_order_summary")
  val orderbyMerchant = view.keyBy(f => (f.merchantid,f.productid))
  println(orderbyMerchant.take(2))
*/
  //sql komutlarini calistirmak icin sparkContext'i cassandraSQLContext'e ceviriyorum.
  val cc = new CassandraSQLContext(sc)

  //product_order_summary tablosundan dataframe olarak tüm kolonlari çekiyoruz.
  val orderDataFrame = cc.sql("select merchantid,productid,year,month,week,day,attrs,channel,disprice,eventcount,finalprice,price,productcount,sellerid,stock from dyna.product_order_summary")
  //Daha kolay islemek için dataframe'i RDD'ye çevirdim.
  val orderRdd = orderDataFrame.rdd
  //Test amaçli tuple olusturmak için keyBy kullanildi.
  //val orderByMerchantAndProduct = orderRdd.keyBy(f => (f.getAs("merchantid"),f.getAs("productid")))
  // orderByMerchantAndProduct.take(100).foreach(println)
  //product_view_summary tablosundan dataframe olarak tüm kolonlari çekiyoruz.
  var viewDataFrame = cc.sql("select merchantid,productid,year,month,week,day,brand,channel,disprice,eventcount,fastseller,price,productcount,scorefive,scorefour,scoreone,scoretwo,scorethree,sellerid from dyna.product_view_summary")

  /*val groupedViewDataFrame = viewDataFrame.groupBy("merchantid",
    "productid",
    "brand",
    "channel",
    "fastseller",
    "sellerid")
    .avg("year",
      "month",
      "week",
      "day",
      "disprice",
      "eventcount",
      "price",
      "productcount",
      "scorefive",
      "scorefour",
      "scorethree",
      "scoretwo",
      "scoreone"
    ).show()
*/
  /*Ürünler transaction olarak birden fazla görüntülenmis olabilir. Bu yüzden ürünleri
  * productid,sellerid göre grupladim. Bu sekilde görüntülenme sayilarini numberOfViews kolonu olarak aldim.*/
  var groupedViewDataFrame = viewDataFrame.groupBy(
    "merchantid",
    "productid",
    "brand",
    "channel",
    "fastseller",
    "sellerid").agg(count("channel").as("numberOfViews1"),
    avg("year").as("year1"),
    avg("month").as("month1"),
    avg("week").as("week1"),
    avg("day").as("day1"),
    avg("disprice").as("disprice1"),
    avg("eventcount").as("eventcount1"),
    avg("price").as("price1"),
    avg("productcount").as("productcount1"),
    avg("scorefive").as("scorefive1"),
    avg("scorefour").as("scorefour1"),
    avg("scorethree").as("scorethree1"),
    avg("scoretwo").as("scoretwo1"),
    avg("scoreone").as("scoreone1")
  )

  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("merchantid","merchantid1")
  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("productid","productid1")
  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("brand","brand1")
  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("channel","channel1")
  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("fastseller","fastseller1")
  groupedViewDataFrame = groupedViewDataFrame.withColumnRenamed("sellerid","sellerid1")

  //groupedViewDataFrame.show()
  //println(groupedViewDataFrame.count())
  /*
  *
  *
  *
  * */
  /*
    val joined = orderDataFrame.join(viewDataFrame,(orderDataFrame("merchantid") <=> viewDataFrame("merchantid")  &&
      orderDataFrame("productid") <=> viewDataFrame("productid") &&
      orderDataFrame("year") <=> viewDataFrame("year") &&
      orderDataFrame("month") <=> viewDataFrame("month")&&
      orderDataFrame("week") <=> viewDataFrame("week")&&
      orderDataFrame("day") <=> viewDataFrame("day")
      )
      ,"left")*/
  //orderDataFrame.show()
  //groupedViewDataFrame.show()

  /*
  *order ve view tablolarinin join islemi. Join isleminden year,month,week ve day çikartildi.Çünkü bu bilgilerin
  * ortalamasi alindi groupedViewDataFrame için.
  * */
  val joined = orderDataFrame.join(groupedViewDataFrame,
    ( orderDataFrame("merchantid") === groupedViewDataFrame("merchantid1")  &&
      orderDataFrame("productid") === groupedViewDataFrame("productid1") &&
      // orderDataFrame("year") === groupedViewDataFrame("year") &&
      // orderDataFrame("month") === groupedViewDataFrame("month")&&
      // orderDataFrame("week") === groupedViewDataFrame("week")&&
      // orderDataFrame("day") === groupedViewDataFrame("day")
      orderDataFrame("channel") === groupedViewDataFrame("channel1") &&
      //orderDataFrame("disprice") === groupedViewDataFrame("disprice") &&
      orderDataFrame("sellerid") === groupedViewDataFrame("sellerid1")
      //  orderDataFrame("price") === groupedViewDataFrame("price")
      )
    ,"left")

  joined.columns.foreach(println)
/*
  joined.select(
    "productid",
   "year","month","week","day","attrs","brand1","channel","disprice","eventcount"
    ,"fastseller1","finalprice","numberOfViews1","price","productcount","scorefive1","scorefour1","scoreone1",
    "scorethree1","scoretwo1","sellerid","stock"
  ).rdd.saveToCassandra("dyna","product_order")
*/
  //joined.foreach(println)
  //println(joined.count())
  //joined.show()
/*
  val a = joined.rdd.map(x => (
    x.getAs("productid"),
    x.getAs("year"),
    x.getAs("month"),
    x.getAs("week"),
    x.getAs("day"),
    x.getAs("attrs"),
    x.getAs("brand1"),
    x.getAs("channel"),
    x.getAs("disprice"),
    x.getAs("eventcount"),
    x.getAs("fastseller1"),
    x.getAs("finalprice"),
    x.getAs("numberOfViews1"),
    x.getAs("price1"),
    x.getAs("productcount"),
    x.getAs("scorefive1"),
    x.getAs("scorefour1"),
    x.getAs("scoreone1"),
    x.getAs("scorethree1"),
    x.getAs("scoretwo1"),
    x.getAs("sellerid"),
    x.getAs("stock"))).saveToCassandra("dyna","product_order")
*/

/*İlk once describe keyspace deyip sonra cassandraya yazmak lazım. Yoksa kolon sıraları uyusmuyor.*/
  val a = joined.rdd.map(x => (
    x.getAs("productid"),
    x.getAs("sellerid"),
    x.getAs("channel"),
    x.getAs("year"),
    x.getAs("month"),
    x.getAs("week"),
    x.getAs("day"),
    x.getAs("attrs"),
    x.getAs("brand1"),
    x.getAs("disprice"),
    x.getAs("eventcount"),
    x.getAs("fastseller1"),
    x.getAs("finalprice"),
    x.getAs("numberOfViews1"),
    x.getAs("price1"),
    x.getAs("productcount"),
    x.getAs("scorefive1"),
    x.getAs("scorefour1"),
    x.getAs("scoreone1"),
    x.getAs("scorethree1"),
    x.getAs("scoretwo1"),
    x.getAs("stock")
    )).saveToCassandra("dyna","product_order")
  // println("joined" + joined.count())

  //println("orderdataFrame" + orderDataFrame.count())
  //println("viewdataFrame" + viewDataFrame.count());
  //a.foreach(println)

  /*
  val viewRdd = viewDataFrame.rdd

  val viewByMerchantAndProduct = viewRdd.keyBy(f => (f.getAs("merchantid"),f.getAs("productid")))


  val joined = orderByMerchantAndProduct.join(viewByMerchantAndProduct)

  val joinedOrders = joined.map(f => (f._2._2.getAs("merchantid"),f._2._2.getAs("productid")))*/
}