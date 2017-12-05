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
    
    //val tweetsHashtagAverage: RDD[(String, Double)] = tweetsOnlyHashtag.reduceByKey((x, y) => x + y)
    //val tweetsHashtagCount: RDD[(String, Integer)] = 
    
    
  }
}