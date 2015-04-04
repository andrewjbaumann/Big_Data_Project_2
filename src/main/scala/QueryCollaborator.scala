/**
 * Title:       QueryCollaborator.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Modified on: 3/27/2015
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
 *              To run:     scala Collaborator input1.txt input2.txt input3.txt input4.txt input5.txt input6.txt
 */

import org.anormcypher._

class QueryCollaborator {

  implicit val connection = Neo4jREST()
  private var user:String = ""
  private var distance:Double = 0

  def start():Unit = {
    query()

    return
  }

  def query():Unit = {
    var valid:Boolean = false
    var response:String = ""

    print("(Y/y) to query database for users with similar skills within a bound distance: ")
    response = Console.readLine()

    if(response == "y" || response == "Y") {
      valid = true
    }

    if(valid == true) {
      print("Enter user id: ")
      user = Console.readLine()
      print("Enter distance: ")
      distance = Console.readDouble()

      val comm = Cypher(
        """
          START sn=node(*)
          WHERE sn.UID={x}
          MATCH (sno:OrganizationNode)
          WHERE (sn-->sno)
          MATCH (u:UserNode), (o:OrganizationNode), (o-[r:DISTANCE_TO]-sno)
          WHERE (sn<>u) AND (r.Distance < {y}) AND ((u-->o) OR (u-->sno))
          MATCH (i:InterestNode), (s:SkillNode)
          WHERE (sn-->i) AND (sn-->s) AND ((u-->i) OR (u-->s))
          RETURN u.UID as id, s.SName as skill, o.OName as organ, i.IName as inter
        """
        ).on("x" -> user, "y" -> distance)

      val commStream = comm()

      println("Format:")
      println("((((USERID, SKILL),ORGANIZATION),INTEREST)")
      println(commStream.map(row =>{row[String]("id")->row[String]("skill")->row[String]("organ")->row[String]("inter")}).toList)

      query()
    }
    else {
      return
    }
  }
}