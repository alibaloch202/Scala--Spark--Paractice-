package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SumOfNumbersProblem {

  def main(args: Array[String]) {
     Logger.getLogger("org").setLevel(Level.OFF)
    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    val conf = new SparkConf().setAppName("sum").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val prime_num = sc.textFile("in/prime_nums.text")
    // splits the RDD form white spaces and takes a list of numbers between spaces
    val primeNum = prime_num.flatMap(line => line.split("\\s+"))

    // filter out the white space so that they won't be involved
    val validNumbers = primeNum.filter(number => !number.isEmpty)

    // convert the strings into integer
    val intnumber = validNumbers.map(number => number.toInt)

    // now sum up the whole RDD
    val product = intnumber.reduce((x, y )=> x + y)
    println("Sum is:" + product)

  }
}
