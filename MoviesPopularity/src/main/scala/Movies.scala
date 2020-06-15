import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark._

import scala.io.{Codec, Source}

object Movies {

  def loadMoviesNames() : Map[Int, String] ={
    //Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for(line <- lines){
      var fields = line.split('|')
      if(fields.length > 1){ //Not blank
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MoviesPopularity")

    var nameDict = sc.broadcast(loadMoviesNames)
    val lines = sc.textFile("../ml-100k/u.data")

    //Tuple (MovieID, 1)
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    val movieCounts = movies.reduceByKey( (x,y) => x + y)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    val sortedMovies = flipped.sortByKey()

    //Fold in the movie names from the broadcast variable
    val sortedMoviesNames = sortedMovies.map( x => (nameDict.value(x._2), x._1) )

    val results = sortedMoviesNames.collect()

    results.foreach(println)


  }
}
