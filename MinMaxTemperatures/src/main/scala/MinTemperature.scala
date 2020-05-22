import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math.min
import scala.math.max

object MinTemperature {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toInt - 273
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val lines = sc.textFile("../1800.csv")

    val parsedLines = lines.map(parseLine)

    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    val stationTemps = minTemps.map(x => (x._1, x._3.toInt))

    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y) )

    val results = minTempsByStation.collect()

    for( result <- results.sorted){
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp C"
      println(s"$station minimum temperature: $formattedTemp")
    }

  }
}
