package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}



object AverageHousePriceProblem {

  def main(args: Array[String])  {


    /* Create a Spark program to read the house data from in/RealEstate.csv,
       output the average price for houses with different number of bedrooms.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

       3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000. */
            Logger.getLogger("org").setLevel(Level.ERROR)

            val conf = new SparkConf().setAppName("housePrice").setMaster("local[3]")
            val sc = new SparkContext(conf)

            val house = sc.textFile("in/RealEstate.csv")
            val header = house.filter(line => !line.contains("Bedroom"))
            val houseMap = header.map(line =>(line.split(",")(3).toInt, (1, line.split(",")(2).toDouble)))
            //total price of each house classified by their bedroom, mor the value of total house price more the house type is popular and in demand
            val housePriceT = houseMap.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2 ))

            println("housePriceT: ")
            for ((bedroom, total)<- housePriceT.collect()) println(bedroom + ":" +total)

            val housepriceavg = housePriceT.mapValues(avgCount => avgCount._2/ avgCount._1)
            println("Average House Price: ")
            val sortedHousePriceAvg = housepriceavg.sortByKey(ascending = false)
            for((bedroom, avg)<- sortedHousePriceAvg.collect()) println(bedroom + ":" + avg)

     // val houseMapF = houseMap.filter()
  }

}
