package com.ilwllc.sgerke.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of Tweets and identifies most popular twitter handle
 *  included with the designated keyword over a 5 minute window.
 */
object TwitterHandle {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    //val sparkConf = new SparkConf().setAppName("PopularHashtags").setMaster("local[2]")
    //val ssc = new StreamingContext(sparkConf, Seconds(1))
    val ssc = new StreamingContext("local[2]","Twitter", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    //val tweets = TwitterUtils.createStream(ssc, None)
    val tweets = TwitterUtils.createStream(ssc, twitterAuth = None)
    val tweets2 = tweets.filter(_.getLang()=="en")
    
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets2.map(status => status.getText())
    val statuses2 = statuses.map(status2 => status2.toLowerCase())
    
    // Now eliminate anything that's does not contrain the word
    val statusContains = statuses2.filter(status => status.contains("nhl"))
    
    // Blow out each word into a new DStream
    val tweetWords = statusContains.flatMap(tweetText => tweetText.split(" "))
    val tweetWords2 = tweetWords.filter(tweetText2 => tweetText2.contains("@"))
    
    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val tweetWordsKeyValues = tweetWords2.map(word => (word, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = tweetWordsKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(3600), Seconds(1))
    
    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/home/steve/spark/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
