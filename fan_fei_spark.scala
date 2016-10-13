/**
  * Created by Franz on 10/11/16.
  */


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd

import scala.util.parsing.json._


object Fan_Fei_Spark {

  var sentiMap = collection.mutable.Map[String, String]()

  def wordStandize(word: String): String = {
    val trimedWord = word.trim
    val firstChar = trimedWord.charAt(0)
    if (firstChar == '@' || firstChar == '#')
      return ""
    var lowerWord = trimedWord.toLowerCase
    if (lowerWord.contains("://") || lowerWord.contains("rt@") || lowerWord == "rt")
      return  ""

    return """^\W+|\W+$""".r replaceAllIn(lowerWord, "")
  }

  def toUTF (word: String): String = {
    return java.net.URLEncoder.encode(word.replaceAll("\\p{C}", ""), "utf-8")
  }


  def getTokens(line: String): Array[String] = {
    val pattern1 = """^\('(.*)', '(.*)'\)$|^\('(.*)', "(.*)"\)$|^\("(.*)", '(.*)'\)$|^\("(.*)", "(.*)"\)$""".r
    val pattern1(a1,a2,a3,a4,a5,a6,a7,a8) = line

    var tokens = new Array[String](2)
    if (a1 != null || a2 != null) {
      tokens(0) = a1
      tokens(1) = a2
    }
    else if (a3 != null || a4 != null) {
      tokens(0) = a3
      tokens(1) = a4
    }
    else if (a5 != null || a6 != null) {
      tokens(0) = a5
      tokens(1) = a6
    }
    else if (a7 != null || a8 != null) {
      tokens(0) = a7
      tokens(1) = a8
    }

    return tokens
  }


  def main(arg: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Fan_Fei_Spark").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val textActress = sc.textFile("actress")
    val textDirector = sc.textFile("director")
    val support = 5

//    val textActress = sc.textFile(arg(0))
//    val textDirector = sc.textFile(arg(1))
//    val support = arg(2).toInt


    var stdActress = textActress
      .map(line => getTokens(line))
      .map(token=>(token(0), token(1)))

    var stdDirector = textDirector
      .map(line => getTokens(line))
      .map(token=>(token(0), token(1)))


    val actress_actress = stdActress
      .join(stdActress)
      .filter(kv => kv._2._1 < kv._2._2)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      //.sortBy(kv => kv._1)
      //.foreach(line => println(line))

    val director_director = stdDirector
      .join(stdDirector)
      .filter(kv => kv._2._1 < kv._2._2)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      //.sortBy(kv => kv._1)
      //.foreach(line => println(line))


    val actress_director = stdActress
      .join(stdDirector)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      //.sortBy(kv => kv._1)
      //.foreach(line => println(line))

    val res = (actress_actress ++ actress_director ++ director_director)
      .sortBy(kv => kv._2)

    //res.foreach(println)

    res.saveAsTextFile("fan_fei_spark")



  }
}