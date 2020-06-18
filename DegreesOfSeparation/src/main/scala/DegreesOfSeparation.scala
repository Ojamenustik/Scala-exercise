import  org.apache.spark._
import  org.apache.spark.SparkContext._
import  org.apache.spark.rdd._
import  org.apache.spark.Accumulator._
import  org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

object DegreesOfSeparation {

  val startCharacterID = 5306
  val targetCharacterID = 14

  var hitCounter:Option[Accumulator[Int]] = None

  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)

  def convertToBFS(line: String): BFSNode = {

    val fields = line.split("\\s+")
    val heroID = fields(0).toInt
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for( connection <- 1 to (fields.length - 1)){
      connections += fields(connection).toInt
    }

    var color:String = "WHITE"
    var distance:Int = 9999

    if(heroID == startCharacterID){
      color = "GRAY"
      distance = 0
    }

    return (heroID, (connections.toArray, distance, color))
  }

  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    val inputFile = sc.textFile("../marvel-graph.txt")
    return inputFile.map(convertToBFS)
  }

  def bfsMap(node:BFSNode):Array[BFSNode] = {
    val characterID:Int = node._1
    val data:BFSData = node._2

    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3

    var results:ArrayBuffer[BFSNode] = ArrayBuffer()

    if(color == "GRAY"){
      for(connection <- connections){
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"

        if(targetCharacterID == connection){
          if(hitCounter.isDefined){
            hitCounter.get.add(1)
          }
        }

        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }
      color = "BLACK"
    }
    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    return results.toArray
  }





  def main(args: Array[String]): Unit = {


  }

}
