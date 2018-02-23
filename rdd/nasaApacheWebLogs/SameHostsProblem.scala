package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    val conf = new SparkConf().setAppName("samehosts").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val julylog1 = sc.textFile("in/nasa_19950701.tsv")
    val auglog2 = sc.textFile("in/nasa_19950801.tsv")

    //picking up the first column  i.e. "host"
    val julylog = julylog1.map(line => line.split("\t")(0))
    val auglog = auglog2.map(line => line.split("\t")(0))

    val intersectlog = julylog.intersection(auglog)
    //removing the header i.e. "host"
    val samehost = intersectlog.filter(host => host != "host")
    //val cleanlog = intersectlog.filter(line => isNotHeader(line))

    samehost.saveAsTextFile("out/nasa_log_same_host.csv")


  }
  def isNotHeader(line: String): Boolean= !(line.startsWith("host")&& line.contains("bytes"))
}
