/**
 * Title:       DbLoader.scala
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

class DbLoader(args:Array[String]) {

  implicit val connection = Neo4jREST()
  private var csvFiles:Array[String] = args

  def start():Unit = {
    Cypher(
    """
        MATCH n-[r]->m
        DELETE r, m, n
    """).execute()

    println("BUILDING DATABASE...")

    createUsers(csvFiles(0))
    createSkills(csvFiles(1))
    createInterests(csvFiles(2))
    createProjects(csvFiles(3))
    createOrganizations(csvFiles(4))
    createDistances(csvFiles(5))

    println("DATABASE BUILD COMPLETE \n")

    return
  }

  def createUsers(file:String):Unit = {
    Cypher(
      """
        LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/user.csv' AS line
        CREATE(uu:UserNode {UID:line[0], FName:line[1], LName:line[2]})
      """).execute()
    Cypher(
      """
        MATCH (u:UserNode)
        WHERE u.UID = 'User id'
        DELETE u
      """).execute()

    return
  }

  def createSkills(file:String):Unit = {
    Cypher(
      """
        LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/skill.csv' AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(ss:SkillNode {SName:line[1]})
        CREATE (u)-[r:SKILLED{SLevel:toFloat(line[2])}]->(ss)
      """).execute()
    Cypher(
      """
        MATCH (s:SkillNode)
        WHERE s.SName = 'Skill '
        DELETE s
      """).execute()

    return
  }

  def createInterests(file:String):Unit = {
    Cypher(
      """
        LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/interestTest.csv' AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(ee:InterestNode {IName:line[1]})
        CREATE (u)-[r:INTERESTED{ILevel:toFloat(line[2])}]->(ee)
      """).execute()
    Cypher(
      """"
        MATCH (i:InterestNode)
        WHERE i.IName = 'Interest'
        DELETE i
      """).execute()

    return
  }

  def createProjects(file:String):Unit = {
    Cypher(
      """
        LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/project.csv' AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(pp:ProjectNode {PName:line[1]})
        CREATE (u)-[r:WORKS_ON]->(pp)
      """).execute()
    Cypher(
      """"
        MATCH (p:ProjectNode)
        WHERE p.PName = 'Project'
        DELETE p
      """).execute()

    return
  }

  def createOrganizations(file:String):Unit = {
      Cypher(
        """
          LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/organizationTest.csv' AS line
          MATCH (u:UserNode)
          WHERE u.UID = line[0]
          MERGE(oo:OrganizationNode {OName:line[1], OType:line[2]})
          CREATE (u)-[r:BELONGS_TO]->(oo)
        """).execute()
      Cypher(
        """
          MATCH (o:OrganizationNode)
          WHERE o.OName = 'organization'
          DELETE o
        """).execute()

    return
  }

  def createDistances(file:String):Unit = {
    Cypher(
      """
        LOAD CSV FROM 'file:///Users/Tony/IdeaProjects/CSCI_493_Project_2/distance.csv' AS line
        MATCH (o1:OrganizationNode),(o2:OrganizationNode)
        WHERE o1.OName = line[0] AND o2.OName = line[1]
        CREATE (o1)-[r:DISTANCE_TO{Distance:toFloat(line[2])}]->(o2)
      """).execute()

    return
  }
}