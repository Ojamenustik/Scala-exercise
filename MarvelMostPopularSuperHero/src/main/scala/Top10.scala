import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Top10 {

  //Function to extract the heroID and number of connections from each line
  def countCoOcurrences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  //Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[ (Int, String)] = {
    var fields = line.split("\"")
    if(fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None //flatmap will just discard None results and extract data from Some results

    }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MarvelMostPopularHero")

    //build up a hero ID -> name RDD
    val names = sc.textFile("../marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    //load up the superhero co-apperrance data
    val lines = sc.textFile("../marvel-graph.txt")

    //convert to (heroID, nember of connections) RDD
    val pairings = lines.map(countCoOcurrences)

    //combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

    //Flip it to connections, heroId
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    //Fined the max of connections

    val top = flipped.top(10)
    var c:Int = 1
    //Look up the name
    for (x <- top) {
    val topName = namesRdd.lookup(x._2)(0)
      println(s"$topName is the $c popular superhero with ${x._1} connections")
      c+=1
    }



  }
}
