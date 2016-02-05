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
    //val conf = new SparkConf().setAppName("Exercise SparkSQL: people info calculator2")
    val conf = new SparkConf().setAppName("pc2")
    val sc=new SparkContext(conf)
    val peopleDataRDD=sc.textFile(arges(0))
    val sqlCon=new SQLContext(sc)
    //this is used to implicityly convert an RDD to a DataFrame.
    import sqlCon.implicits._
    val schemaArray=schemaString.split(",")
    val schema=StructType(schemaArray.map(fieldName=>StructField(fieldName,StringType,true)))
    //==for test
    peopleDataRDD.map(_.split(" ")).collect().foreach(a=>println(a))
    //==end for test
    val rowRDD: RDD[Row]=peopleDataRDD.map(_.split(" ")).map(eachRow=>Row(eachRow(0),eachRow(1),eachRow(2)))
    val peopleDF=sqlCon.createDataFrame(rowRDD,schema)
    peopleDF.registerTempTable("people")
    //get the male people whose height is more than 180
    val higherMale180=sqlCon.sql("select id, gender,height from people where height>180 and gender='M'"
    )
    println("Men whose height are more than 180: "+higherMale180.count())
    println("<Display #1>")
    //get the female people whose height is more than 170
    val higherFemale170 =sqlCon.sql("select id,gender,height from people where height>170 and gender='F'")
    println("Women whose height are more than 170: "+higherFemale170.count())
    println("<Display #2>")
    //grouped the people by gender and count the number
    peopleDF.groupBy(peopleDF("gender")).count().show()
    println("People count group by gender")
    println("<Display #3>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(
      peopleDF("height")>210).show(50)
    println("Men whose height is more than 210")
    println("<Display #4>")
    //
    peopleDF.sort($"height".desc).take(50).foreach{
      row=>println(row(0)+", "+row(1)+", "+row(2))}
    println("Sorted the people by height in descend order, show top 50 people")
    println("<Display #5>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("F")).agg(Map("height"->"avg")).show()
    println("The average height for women")
    println("<Display #6>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("M")).agg("height"->"max").show()
    println("The max height of men")
    println("<Display #7>")

    println("All the calculation is done.")

  }
}
