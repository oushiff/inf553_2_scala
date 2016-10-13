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
    val pattern1 = """^\('(.+)', '(.+)'\)$|^\('(.+)', "(.+)"\)$|^\("(.+)", '(.+)'\)$|^\("(.+)", "(.+)"\)$""".r
    val pattern1(a1,a2,a3,a4,a5,a6,a7,a8) = line

    var tokens = new Array[String](2)
    var index = 0
    if (a1 != null) {
      tokens(index) = a1
      index += 1
    }
    if (a2 != null) {
      tokens(index) = a2
      index += 1
    }
    if (a3 != null) {
      tokens(index) = a3
      index += 1
    }
    if (a4 != null) {
      tokens(index) = a4
      index += 1
    }
    if (a5 != null) {
      tokens(index) = a5
      index += 1
    }
    if (a6 != null) {
      tokens(index) = a6
      index += 1
    }
    if (a7 != null) {
      tokens(index) = a7
      index += 1
    }
    if (a8 != null) {
      tokens(index) = a8
      index += 1
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


//    for (line <- textActress) {
//      val tokens = getTokens(line)
//      tokens.foreach(println)
//      println("\n\n")
//
//    }


//    val res = textActress
//      .map(line => getTokens(line))
//      .reduce(_ ++ _)
//      .groupBy( _(0))
//


    var stdActress = textActress
      .map(line => getTokens(line))
      .map(token=>(token(0), token(1)))

    var stdDirector = textDirector
      .map(line => getTokens(line))
      .map(token=>(token(0), token(1)))


//    res111.foreach(println)

    val actress_actress = stdActress
      .join(stdActress)
      //.map(kv => (kv._1, kv._2))
      .filter(kv => kv._2._1 < kv._2._2)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      .sortBy(kv => kv._1)
      .foreach(line => println(line))

    println("^^^^^^^^^^^^^^^\n\n")


    val director_director = stdDirector
      .join(stdDirector)
      //.map(kv => (kv._1, kv._2))
      .filter(kv => kv._2._1 < kv._2._2)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      .sortBy(kv => kv._1)
      .foreach(line => println(line))

    //res222.map(line => println(line))////.foreach(println)

    println("^^^^11111111^^^^^^^^^^^\n\n")

    val actress_director = stdActress
      .join(stdDirector)
      //.map(kv => (kv._1, kv._2))
      //.filter(kv => kv._2._1 < kv._2._2)
      .map(kv => ((kv._2._1, kv._2._2), 1))
      .reduceByKey(_ + _)
      .filter(kv => kv._2 > support)
      .sortBy(kv => kv._1)
      .foreach(line => println(line))



  //  res111.foreach(println)
    //      .map( kv => (kv._1, kv._2.map())


    //res.foreach(println)
//    res.foreach(line => line.foreach(println))


    //    val lines = textTwitter
//      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))
//      .map(line=>{
//        line
//          .split(" ")
//          .map(word=>sentiMap.getOrElse(toUTF(wordStandize(word)), "0").toInt)
//          .reduce(_ + _)
//      })
//      .zipWithIndex()
//      .map(input=>(input._2 + 1, input._1))
//
//    //lines.foreach(println)
//
//    lines.saveAsTextFile("fei_fan_tweets_sentiment_first20")
//


  }
}