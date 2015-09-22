/**
 * Created by Venu on 9/22/15.
 */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SampleSparkIOS {

  def main(args: Array[String]) {


    val filters = args

    System.setProperty("twitter4j.oauth.consumerKey", "8otaSfYl7UlqQYfDzAsKCIKti")
    System.setProperty("twitter4j.oauth.consumerSecret", "CJQ1xjRHdWm1C1sRy5E5BoHiBAQtvC2BzbKMUafXKMxfQnEIHk")
    System.setProperty("twitter4j.oauth.accessToken", "149520055-jENEE4zYfvQpaL2Mw5xggoTHYOq7PQqdWEx341bv")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "vBVVYXP67pPMdMj8BCaUC1LFQ0peFXfgpg4O5Ue1dsK6N")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")


    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 10 second window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(1)
      SocketClient.sendCommandToRobot( rdd.count() +"tweets analyzed in 10 seconds window")
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    ssc.start()

    ssc.awaitTermination()
  }


}
