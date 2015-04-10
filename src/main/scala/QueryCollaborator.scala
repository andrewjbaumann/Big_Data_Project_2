/**
 * Title:       QueryCollaborator.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Modified on: 4/4/2015
 * Description: Program that will parse multiple csv files and communicate with a neo4j database using cypher query
 *              language.
 *              1. You need to write three programs in one of the following languages: scala, java, python, or C++,
 *              although scale is preferred. The first program called DbLoader will load the data into a database.
 *              The second and third programs are called QueryCollaborator and QueryColOfCol, respectively.
 *              2. The input for DbLoader is a fold that includes six files for the data. In project fold of the
 *              blackboard, there is a compressed sample data fold. All you input data should use the same file names,
 *              headers, and formats. Although only a few records in the sample data, I will test your programs using
 *              ~1 million records.
 *              3. The input of QueryCollaborator is a user id and a distance. The outputs are a list of user names,
 *              their common interests (skills) shared with the query user, ranked the interest (skill) weights. See
 *              lecture notes for more details.
 *              4. The input of QueryColOfCol is a user id. The outputs are a list of user names. See lecture nodes for
 *              more details.
 * Build with:  Scala IDE (Eclipse or IntelliJ) or using the following commands on the glab machines
 *              To compile: scalac *.scala
 *              To run:     scala Collaborator 'fileLocation'
 * Notes:       Completed(?) - Andrew
 */

import org.anormcypher._
//import scala.collection.mutable._
import scala.collection.immutable._


/**
 * QueryCollaborator class that asks the user for a user id and a distance to find other users with common skills and
 * interests. Output will be ordered by the weight of total skills and interests
 */
class QueryCollaborator {

  /**
   * Connects to the neo4j database (version run on my machine does not require authentication, whereas other versions
   * may require a different setup).
   */
  //implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")
  implicit val connection = Neo4jREST("localhost", 7474, "/db/data/", "neo4j", "neo4j")

  private var user, organizationType:String = ""
  private var distance:Double = 0

  /**
   * Method that calls the query method.
   */
  def start():Unit = {
    query()

    return
  }

  /**
   * Tail recursive method that loops, asking the user if they want to query the database for users with similar skills
   * and/or interests as a user id.
   */
  def query():Unit = {
    var valid:Boolean = false
    var response:String = ""

    print("(Y/y) to query database for users with similar skills and/or interests within a bound distance: ")
    response = Console.readLine()

    if(response == "y" || response == "Y") {
      valid = true
    }

    if(valid == true) {
      print("\tEnter user id: ")
      user = Console.readLine()
      print("\tEnter organization type: ")
      organizationType = Console.readLine().toUpperCase
      print("\tEnter distance: ")
      distance = Console.readDouble()

      /**
       * Cypher query to find colleagues of an organization who share similar skills and/or interests within a given
       * distance.
       */
      val comm = Cypher(
        """
          MATCH (user:UserNode{UID:{x}}), (oo:OrganizationNode), ((o:OrganizationNode)-[d:DISTANCE_TO]-(userOrg:OrganizationNode{OType:UPPER({type})})), ((u:UserNode)-[r:INTERESTED|SKILLED]-(is))
          WHERE (user <> u) AND (user-->userOrg) AND (d.Distance <= {y}) AND ((u-->o) OR (u-->userOrg)) AND (u-->is<--user) AND (u-->oo)
          RETURN "User:" +u.UID + ". Organization:" + oo.OName + ". Weight: " as ido, is.Name as isName,  r.Level as level
        """).on("x" -> user, "y" -> distance, "type" -> organizationType)

      val commStream = comm()
      val results = commStream.map(row =>{row[String]("ido")->row[String]("isName")->row[Int]("level")}).toSet
      val myResults = results.groupBy(it => it._1._1)

      println("\nThis is the list of nodes with similar interests:")
      var resultsList : List[(String,Int,String)] = List()
      iterate(myResults)

      def iterate(aMap : Map[String, Set[(((String),String),Int)]]) : Unit ={
        if(aMap.size == 0)
          return

        var sum : Int = 0;
        var skit : String = " ";

        aMap.head._2.foreach(it => sum = sum + it._2)
        aMap.head._2.foreach(it => skit = skit + it._1._2 + "&")

        resultsList = resultsList :+ (aMap.head._1, sum, skit)

        iterate(aMap.tail)
      }

      resultsList = resultsList.sortBy(it => it._2)
      resultsList.foreach(println)

      /*println("Iteration:")
      iterateSet(results)
      def iterateSet(aSet : Set[((((String,String),String),Int))], count : Int): Unit = {
        if (0 == aSet.size)
          return
        aSet.head._1
        println(count, " and ", aSet.head._1)
        iterateSet(aSet.tail)
      }
      println("Stop")
      */


      /**
       * Prints out a mapped list of returned values from the cypher query.
       */
     //println("\t" + myResults + "\n")

      /**
       * Tail recursively calls itself
       */
      query()
    }

    /**
     * If Boolean check fails, it ends the loop.
     */
    else {
      return
    }
  }
}
