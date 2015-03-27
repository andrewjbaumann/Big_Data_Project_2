/**
 * Title:       Parser.scala
 * Authors:     Andrew Baumann, Tony Zheng
 * Modified on: 3/27/2015
 * Description: Program that will parse multiple csv files and communicate with a neo4j database using cypher query
 *              language.
 *              1. You need to write three programs in one of the following languages: scala, java, python, or C++,
 *              although scale is prefered. The first program called DbLoader will load the data into a database.
 *              The second and third programs are called QueryCollabrator and QueryColOfCol, respectively.
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

import scala.io.Source
import org.anormcypher._

class Parser(file:String) {
  implicit val connection = Neo4jREST()

  private val fileName = file

  def Parsing():Unit = {
    fileName match {
      case "user.csv" => userCSV()
      case _ => println("nope")
    }
  }

  def userCSV():Unit = {
    println("user")

    Cypher("LOAD CSV WITH HEADERS FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/user.csv' AS line create(:USER {uID: line.Userid, fName: line.Firstname, lName: line.Lastname})").execute()

    println("graces vag")
  }
    //Cypher("create (User {name:'Hunter College'})").execute()
    //val req = Cypher("start n=node(*) return n.fName")
    //Cypher("match (n) delete n").execute()
    //val stream = req()
    //println(stream.map(row =>{row[String]("n.fname")}).toList)


}
