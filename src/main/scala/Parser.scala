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

    //Cypher("MATCH n DELETE n").execute()
    println(fileName)
    fileName match {
      case "distance.csv"     => distanceCSV()
      case "interest.csv"     => interestCSV()
      case "organization.csv" => organizationCSV()
      case "project.csv"      => projectCSV()
      case "skill.csv"        => skillCSV()
      case "user.csv"         => userCSV()
      case _                  => println("nope")
    }
  }

  def distanceCSV():Unit = {

  }

  def interestCSV():Unit = {

  }

  def organizationCSV():Unit = {

  }

  def projectCSV():Unit = {

  }

  def skillCSV():Unit = {
    //Cypher("CREATE(ss:SKILL {SName: s1}").execute()
    Cypher("LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/skill.csv' AS line MATCH (a:UserNode) WHERE a.UID = line[0] MERGE(ss:SkillNode {SName:line[1]}) CREATE (a)-[r:SKILLED{SLevel:line[2]}]->(ss)").execute()
    //Cypher("LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/skill.csv' AS line MATCH (d:SKILL {SkillName:line[1]}) WITH d SKIP 1 DELETE d").execute()
    Cypher("""MATCH (n:SkillNode) WHERE n.SName = 'Skill ' DELETE n""").execute()
    //Cypher("LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/skill.csv' AS line CREATE(ss:SKILL {SkillName:line[1]}) MATCH(a:USER) WHERE a.UID = line[0] MATCH(b:SKILL) WHERE b.SkillName = line[1] CREATE (a)-[r:SKILLED]->(b)").execute()
    //Cypher("LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/skill.csv' AS line CREATE(ss:SKILL {SkillName:line[1]}) MATCH (a:USER),(b:SKILL) WHERE a.UID = line[0] AND b.SkillName = line[1] CREATE (a)-[r:'IS_SKILLED_AT']->(b)").execute()
    //Cypher("MATCH s WHERE s.UID = 'Skill' DELETE s").execute()

  }

  def userCSV():Unit = {
    Cypher("MATCH n DELETE n").execute()
    Cypher("LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/user.csv' AS line CREATE(uu:UserNode {UID:line[0], FName:line[1], LName:line[2]})").execute()
    Cypher("MATCH (n:UserNode) WHERE n.UID = 'User id' DELETE n").execute()
  }

  /*def getUserResults():Unit = {
    //Cypher("create (User {name:'Hunter College'})").execute()
    val req = Cypher("start n=node(*) return n.FName")
    val stream = req()
    println(stream.map(row =>{row[String]("n.FName")}).toList)
  }*/
}
