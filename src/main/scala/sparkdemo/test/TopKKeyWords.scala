package sparkdemo.test

/**
  * Created by ayang on 2016/2/1.
  * refer: https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice1/
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TopKKeyWords {
  def main(args:Array[String]){
    if(args.length<2){
      println("Need TopKKeyWords file and K")
      System.exit(1)
    }
    val conf =new SparkConf().setAppName("Spark Exercise: Top K Key Words")
    val sc=new SparkContext(conf)

    val kwordData=sc.textFile(args(0))
    val countedData =kwordData.map(line=>(line.toLowerCase(),1)).reduceByKey((a,b)=>a+b)

    val sortedData=countedData.map{case (k,v)=>(v,k)}.sortByKey(false)
    val topKData=sortedData.take(args(1).toInt).map{case (v,k)=>(k,v)}
    topKData.foreach(println)

  }
}
