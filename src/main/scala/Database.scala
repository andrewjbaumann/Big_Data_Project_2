/**
 * Title:       Database.scala
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

import org.anormcypher._

class Database(args:Array[String]) {

  private var csvFiles:Array[String] = args

  def start():Unit = {
    createUsers(csvFiles(0))
    createSkills(csvFiles(1))
  }

  def createUsers(file:String):Unit = {
    val usersCSV = new Parser(file)
    usersCSV.Parsing()
  }

  def createSkills(file:String):Unit = {
    val skillsCSV = new Parser(file)
    skillsCSV.Parsing()
  }

}
