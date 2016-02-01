package sparkdemo.test

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ayang on 2016/1/29.
  * refer: https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice1/
  */
object PeopleInfoCalculator {
  def main(arges:Array[String]){
    if(arges.length<1){
      println("Need file for peopleinfo")
      System.exit(1)
    }
    val conf=new SparkConf().setAppName("Spark Exercise: Peopeo Info Calculater (Gender & Height)")
    val sc =new SparkContext(conf)
    val dataFile=sc.textFile(arges(0),5)
    val maleData=dataFile.filter(line=>line.contains("M")).map(
      line=>(line.split(" ")(1)+" "+line.split(" ")(2)))
    val femaleData=dataFile.filter(line=>line.contains("F")).map(
      line=>(line.split(" ")(1)+" "+line.split(" ")(2)))

    val maleHeightData=maleData.map(line=>line.split(" ")(1).toInt)
    val femaleHeightData=femaleData.map(line=>line.split(" ")(1).toInt)

    val lowestMale=maleHeightData.sortBy(x=>x,true).first()
    val lowestFemale=femaleHeightData.sortBy(x=>x,true).first()

    val highestMale=maleHeightData.sortBy(x=>x,false).first()
    val highestFemale=femaleHeightData.sortBy(x=>x,false).first()

    println("Number of Male people: "+maleData.count())
    println("Lowest Male: "+lowestMale)
    println("Highest Male: "+highestMale)
    println("Number of Female people: "+femaleData.count())
    println("Lowest Female: "+lowestFemale)
    println("Highest Female: "+highestFemale)
  }
}
