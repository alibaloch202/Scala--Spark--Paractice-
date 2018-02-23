package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))
    val wordPairRdd = wordRdd.map(word => (word, 1))

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    //flip the wordPair RDD as we need he key i.e. word count as Integar so that It can be sorted
    val countWordPairs = wordCounts.map(wordCount => (wordCount._2, wordCount._1))
    val sortedCount = countWordPairs.sortByKey(ascending = false)
    ///filp back the sorted pairedRDD so that we will got value i.e. text as a key and count i.e. key as a value as RDD is redesing s key and value we have to do the same to flipback it as we did for flip at first
    val sortedCountWord = sortedCount.map(sCount => (sCount._2, sCount._1))
    for ((word, count) <- sortedCountWord.collect()) println(word + " : " + count)
  }
}

