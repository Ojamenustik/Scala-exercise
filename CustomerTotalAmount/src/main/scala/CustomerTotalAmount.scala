import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math.min
import scala.math.max

object CustomerTotalAmount {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerID = fields(0)
    val orderID = fields(1)
    val total = fields(2).toFloat
    (customerID, total)
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerTotal")
    val lines = sc.textFile("../customer-orders.csv")
    val parsedLines = lines.map(parseLine)
    val customersTotals = parsedLines.map(x => (x._1, x._2))
    val customerTotal = customersTotals.reduceByKey((x,y) => x + y)
    val customersTotalsSorted = customerTotal.map(x => (x._2, x._1)).sortByKey().collect()

    for(result <- customersTotalsSorted){
      val customer = result._2
      val total = result._1
      println(s"$customer : $total")
    }

  }

}
