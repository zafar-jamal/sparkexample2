package scalasparkexample2.sparkexample2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
 * Hello world!
 *
 */


object AppObj2  {

   def main (args : Array[String])  {
     
     // print( "Hello World..Hello World!!!!!!..HEllo.." )
      
      myWordCount
    //  justHelloWorld
   }
   
   def justHelloWorld {
      println ("This is just a Hello world program...!")
   }
   
       def myWordCount {
      
      val conf=new SparkConf()
      conf.setAppName("Word count Application")
      conf.setMaster("local")
      
      val sc= new SparkContext(conf)
      
      val textfile= sc.textFile("file:///home/cloudera/workspace/sparkexample2/fruit.text")
      
    
      val wordCounts = textfile.flatMap(line => line.split(" "))
                                .map(word => (word, 1))
                                .reduceByKey((a, b) => a + b)

           
         wordCounts.saveAsTextFile("fruit.count.text")  
      
       }
      
}
