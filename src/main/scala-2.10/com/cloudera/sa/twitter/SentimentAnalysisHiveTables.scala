package com.cloudera.sa.twitter

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.cloudera.sa.twitter.StanfordNlpFunctions._

/**
 * Created by gmedasani on 11/30/16.
 */
object SentimentAnalysisHiveTables {

  def main (args: Array[String]) {

    if(args.length < 5){
      System.err.println("Usage: SentimentAnalysisHiveTables <input-table> <output-table> <partitionYear> <partitionMonth> <partitionDay>")
      System.exit(1)
    }

    //Set the Spark application arguments
    val stagingTable = args(0)
    val targetTable = args(1)
    val partitionYear = args(2)
    val partitionMonth = args(3)
    val partitionDay = args(4)

    //Create Spark configuration and set the connection to the Spark cluster
    val sparkConf = new SparkConf().setAppName("Twitter-Sentiment-Analysis")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.dialect","hiveql")
    sqlContext.setConf("hive.exec.dynamic.partition","true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    //Load the Twitter data from a staging table
    val stagingDF = sqlContext.sql("select * from "+stagingTable).coalesce(10)

    //Split the text into multiple sentences
    val sentences = ssplit(stagingDF.col("tweet_text"))

    //Calculate the number of sentences
    val numOfSentences = ssplitlen(stagingDF.col("tweet_text"))

    //Calculate the document level sentiment on the tweet
    val documentSentiment = documentsentiment(stagingDF.col("tweet_text"))

    //Add the sentences, document level sentiments and sentence level sentiments
    val stagingWithSentencesDF = stagingDF
      .withColumn("sentences",sentences)
      .withColumn("document_sentiment",documentSentiment)
      .withColumn("num_of_sentences",numOfSentences)

    //Calculate the sentence level sentiment on the multiple sentences in a tweet.
    val sentenceSentiments = sentencesentiment(stagingWithSentencesDF.col("sentences"))

    //Add the sentence level sentiments
    val sentimentsDF = stagingWithSentencesDF
      .withColumn("sentence_sentiments",sentenceSentiments)

    //For debugging purposes
    sentimentsDF.show(truncate = false)

    //register the table as a temp table
    sentimentsDF.registerTempTable("twitterbase_staging_with_sentiments")

    //Insert overwrite statement of the twitterbase_sentiment table
    val insertSQLStatement = "INSERT OVERWRITE TABLE "+targetTable+" PARTITION(load_year,load_month,load_day) SELECT " +
      "id," +
      "created_at," +
      "coordinates," +
      "source," +
      "favorited," +
      "retweeted_status_text," +
      "retweeted_status_user_screen_name," +
      "retweeted_status_user_name," +
      "retweeted_status_retweet_count," +
      "expanded_url," +
      "user_mentions_screen_name," +
      "user_mentions_name," +
      "hashtags," +
      "tweet_text," +
      "user_screen_name," +
      "user_name," +
      "user_friends_count," +
      "user_followers_count," +
      "user_statuses_count," +
      "user_verified," +
      "user_geo_enabled," +
      "user_utc_offset," +
      "user_lang," +
      "user_location," +
      "user_time_zone," +
      "tweet_year," +
      "tweet_month," +
      "tweet_day," +
      "tweet_time," +
      "tweet_time_utc," +
      "sentences," +
      "document_sentiment," +
      "num_of_sentences," +
      "sentence_sentiments," +
      "load_year," +
      "load_month," +
      "load_day" +
      " FROM twitterbase_staging_with_sentiments " +
      "WHERE load_year="+partitionYear+" and load_month="+partitionMonth+" and load_day="+partitionDay+""

    System.out.println("================")
    System.out.println(insertSQLStatement)
    System.out.println("================")

    //Insert the data into the twitterbase_sentiment table
    sqlContext.sql(insertSQLStatement)

  }
}
