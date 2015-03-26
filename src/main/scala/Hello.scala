import org.anormcypher._
import scala.util.Either._
import scala.collection.immutable.Stream._

object Hello {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")

    implicit val connection = Neo4jREST()

    Cypher("""create (anorm {name:"AnormCypher"}), (test {name:"Test"})""").execute()
    val req = Cypher("start n=node(*) return n.name")
    val stream = req()
    //stream.map(row => {row[String]("n.name")}).toList
    stream.map(row =>{row[String]("n.name")}).toList
  }
}
