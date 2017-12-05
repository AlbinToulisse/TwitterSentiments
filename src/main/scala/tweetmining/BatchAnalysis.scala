package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")
    
    val tweetsLower: RDD[String] = tweets.map(_.toLowerCase())
    val tweetsTrump: RDD[String] = tweetsLower.filter(_.contains("trump"))
    println("Nombre de tweets contenant trump: " + tweetsTrump.count())
    println("\n")
    
    val tweetsSentiment: RDD[(String, Double)] = tweets.map(text => (text, TweetUtilities.getSentiment(text)))
    tweetsSentiment.take(5).foreach(println)
    println("\n")
    
    val tweetsHashtag: RDD[(String, Double)] = tweetsSentiment.flatMap(pair => TweetUtilities.getHashTags(pair._1).map((_, pair._2)))
    tweetsHashtag.take(5).foreach(println)
    println("\n")
    
    val tweetsHashtagSet: RDD[(String, Set[Double])] = tweetsHashtag.combineByKey(x => Set(x),
        (s: Set[Double], x: Double) => s + x,
        (s1: Set[Double], s2: Set[Double]) => s1 ++ s2)
    val tweetsHashtagAverage: RDD[(String, Double)] = tweetsHashtagSet.map(x => (x._1, (x._2.sum)/x._2.size))
    println("Most positive Hashtag: ")
    val tweetsHashtagSwap: RDD[(Double, String)] = tweetsHashtagAverage.map(_.swap)
    tweetsHashtagSwap.top(5).foreach(println)
    println("Most negative Hashtag: ")
    tweetsHashtagSwap.takeOrdered(5).foreach(println)
    println("\n")
    
    
  }
}