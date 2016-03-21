package sparkdemo.test

/**
  * Created by ayang on 2016/3/21.
  * This is an example of SparkSQL and HiveContext.
  *
  * 1. create hive table
  * 2. do some action on the table
  *
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

object PeopleCal {
  val conf=new SparkConf().setAppName("SparkSQL and HiveContext")
  val sc=new SparkContext(conf)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("CREATE TABLE IF NOT EXISTS TestTable2 (key INT, value STRING)")
  hiveContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE TestTable2")
  hiveContext.sql("from TestTable2 SELECT key, collect_list(value) group by key order by key").collect.foreach(println)

}
