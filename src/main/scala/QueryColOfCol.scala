
/**
 * Title:       QueryColOfCol.scala
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
 * Notes:       Completed(✓) - Tony/Andrew
 */

import org.anormcypher._

/**
 * QueryColOfCol class that asks the user for a user id to find other colleagues of colleagues with common interests.
 */
class QueryColOfCol {

  /**
   * Connects to the neo4j database (version run on my machine does not require authentication, whereas other versions
   * may require a different setup).
   */
  //implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")
  implicit val connection = Neo4jREST("localhost", 7474, "/db/data/", "neo4j", "neo4j")

  private var user:String = ""
  private var particularInterests:List[String] = List()
  private var colOfColList:List[(String, String)] = List()

  /**
   * Method that calls the query method.
   */
  def start():Unit = {
    query()

    return
  }

  /**
   * Tail recursive method that loops, asking the user if they want to query the database for colleagues of colleagues.
   * Trusted colleagues-of-colleagues who have one or more particular interests. The “trusted colleague” is defined as
   * two persons have worked on the same project.
   */
  def query():Unit = {
    var valid, more:Boolean = false
    var response, moreResponse:String = ""

    /**
     * Asks user whether they want to query the database or not.
     */
    print("(Y/y) to query database for collaborators of collaborators with similar interests: ")
    response = Console.readLine()

    /**
     * Boolean check for a confirmation of continuing.
     */
    if(response == "y" || response == "Y") {
      valid = true
    }

    /**
     * If Boolean check is passed, it continues with the query.
     */
    if(valid == true) {

      /**
       * Has user ask for the query user.
       */
      print("\tEnter user id: ")
      user = Console.readLine()

      /**
       * Has user enter in at least one interest for the colleagues of colleagues.
       */
      print("\tEnter a particular interest for the col of col: ")
      particularInterests = particularInterests.:+(Console.readLine().toUpperCase)

      /**
       * Asks user if they want to look for more interests
       */
      print("\n(Y/y) to look for more interests: ")
      moreResponse = Console.readLine()

      if(moreResponse == "y" || moreResponse == "Y") {
        more = true
      }

      /**
       * Continually asks the user if they want to look for more interests for the colleagues of colleagues.
       */
      while(more == true) {
        print("\tEnter a particular interest for the col of col: ")
        particularInterests = particularInterests.:+(Console.readLine().toUpperCase)
        print("\n(Y/y) to look for more interests: ")
        moreResponse = Console.readLine()

        if(moreResponse == "y" || moreResponse == "Y") {
          more = true
        }
        else {
          more = false
        }
      }

      println("\nFinding all colleagues of colleagues who have these interests:")
      println("\t" + particularInterests)

      /**
       * Unwinds interests and finds all cols of cols who have the interests listed.
       */
      val comm = Cypher(
        """
          UNWIND {myList} as partInt
          MATCH (user:UserNode{UID:{x}}), (col:UserNode), (colOfCol:UserNode), (p1:ProjectNode), (p2:ProjectNode), (i:InterestNode{Name:UPPER(partInt)})
          WHERE (user<>col) AND ((user)-->(p1)<--(col)-->(p2)<--(colOfCol)) AND (colOfCol-->i)
          RETURN colOfCol.FName as firstName, colOfCol.LName as lastName, count(colOfCol.UID) as counter
        """).on("x" -> user, "myList" -> particularInterests)

      val commStream = comm()

      /**
       * Prints out a mapped list of returned values from the cypher query while filtering out those who do not have all
       * the interests listed.
       */
      var results = (commStream.map(row => {row[String]("firstName")->row[String]("lastName")->row[Int]("counter")}).toList).filterNot(line => line._2 != particularInterests.size)

      for(x <- results) {
        colOfColList = colOfColList.:+(x._1)
      }

      println("All colleagues of colleagues who have those interests:")
      println("\t" + colOfColList + "\n")

      /**
       * Clears the lists for another query.
       */
      particularInterests = List()
      colOfColList = List()

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