package sparkdemo.test

/**
  * Created by ayang on 2016/1/29.
  * refer: https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice1/
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



object AvgPriceCalculater {

  def main(args:Array[String]) {
    if (args.length < 1) {
      println("Need file for AvgPriceCalculator")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SparkExcercise: Average Price Calculator")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(args(0), 5)
    val count = dataFile.count()
    val priceData = dataFile.map(line => line.split(" ")(1))
    val totalPrice = priceData.map(
      price =>Integer.parseInt(String.valueOf(price))).collect().reduce(
      (a, b) => a + b
    )
    println("Total Price: " + totalPrice + "; Number of Product: " + count)
    val avgPrice: Double = totalPrice.toDouble / count.toDouble
    println("Average Price is " + avgPrice)

  }
}
