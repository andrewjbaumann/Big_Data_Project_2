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

/**
 * QueryCollaborator class that asks the user for a user id and a distance to find other users with common skills and
 * interests. Output will be ordered by the weight of total skills and interests
 */
class QueryCollaborator {

  /**
   * Connects to the neo4j database (version run on my machine does not require authentication, whereas other versions
   * may require a different setup).
   */
  implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")
  private var user:String = ""
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
      print("Enter user id: ")
      user = Console.readLine()
      print("Enter distance: ")
      distance = Console.readDouble()

      /**
       * Cypher query to find colleagues of an organization who share similar skills and/or interests within a given
       * distance.
       */
      val icomm = Cypher(
        """
          START sn = node(*)
          WHERE sn.UID = {x}
          MATCH (sno: OrganizationNode)
          WHERE (sn --> sno)
          MATCH (u: UserNode), (o: OrganizationNode), (o -[r: DISTANCE_TO] -sno)
          WHERE (sn <> u) AND (r.Distance < {y}) AND ((u --> o) OR (u --> sno))
          MATCH (i: InterestNode), (i -[ir:INTERESTED]-u)
          WHERE (u --> i <-- sn)
          RETURN u.UID as id, o.OName as organ, i.IName as inter, ir.ILevel as level
         """.stripMargin
        ).on("x" -> user, "y" -> distance)

      val scomm = Cypher(
        """
          START sn = node(*)
          WHERE sn.UID = {x}
          MATCH (sno: OrganizationNode)
          WHERE (sn --> sno)
          MATCH (u: UserNode), (o: OrganizationNode), (o -[r: DISTANCE_TO] -sno)
          WHERE (sn <> u) AND (r.Distance < {y}) AND ((u --> o) OR (u --> sno))
          MATCH (s: SkillNode), (s-[sr:SKILLED]-u)
          WHERE (u -->s<-- sn)
          MATCH (op: OrganizationNode)
          WHERE (u-->op)
          RETURN u.UID as id, op.OName as organ, s.SName as skill, sr.SLevel as level
        """.stripMargin
      ).on("x" -> user, "y" -> distance)

      val commStreami = icomm()
      val commStreams = scomm()

      /**
       * Prints out a mapped list of returned values from the cypher query.
       */
      println("This is the list of nodes with similar interests:")
      println(commStreami.map(row =>{row[String]("id")->row[String]("organ")->row[String]("inter")->row[BigDecimal]("level")}).toList)

      println("This is a list of the nodes with similar skills:")
      println(commStreams.map(row =>{row[String]("id")->row[String]("organ")->row[String]("skill")->row[BigDecimal]("level")}).toList)



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
