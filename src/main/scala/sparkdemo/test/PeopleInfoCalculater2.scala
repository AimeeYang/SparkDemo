package sparkdemo.test

/**
  * Created by ayang on 2016/2/1.
  * refer: https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice3/
  *
  * use SparkSQL
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

object PeopleInfoCalculater2 {

  private val schemaString="id,gender,height"
  def main(arges:Array[String]){
    if(arges.length<1){
      println("Need people info file")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Exercise SparkSQL: people info calculator2")
    val sc=new SparkContext(conf)
    val peopleDataRDD=sc.textFile(arges(0))
    val sqlCon=new SQLContext(sc)
    //

  }
}
