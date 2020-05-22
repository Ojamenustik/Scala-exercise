import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import scala.math.max

object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("../book.txt")
    val words = input.flatMap(x => x.split("\\W+"))
    val lowerCase = words.map(x => x.toLowerCase())

    //countByValue() the HARD way
    val wordCounts = lowerCase.map(x => (x,1)).reduceByKey((x,y) => x + y)

    //flip (word, count) -> (count, word)
    //sort by count (our new key)
    val wordCountSorted = wordCounts.map(x => (x._2, x._1)).sortByKey().collect()

    for(result <- wordCountSorted){
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}
