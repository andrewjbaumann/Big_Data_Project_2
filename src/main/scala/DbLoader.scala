/**
 * Title:       DbLoader.scala
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
 * Notes:       Completed(✓) - Tony
 */

import scala.io.Source
import org.anormcypher._

/**
 * DbLoader class that takes in a string input (file location of csvs) and runs queries on the neo4j database, creating
 * entities and relations between them.
 */
class DbLoader(fileLoc:String) {

  /**
   * Connects to the neo4j database (version run on my machine does not require authentication, whereas other versions
   * may require a different setup).
   */
  implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")

  /**
   * Method that starts by clearing the database of any relations, entities tied to relations, and lone entities for
   * testing.
   */
  def start():Unit = {
    Cypher(
    """
        MATCH n-[r]->m, o
        DELETE r, m, n, o
    """).execute()

    println("BUILDING DATABASE...")

    /**
     * calls the different methods within this class to run the different queries on the database. In the methods that
     * involve creating entities, since the LOAD CSV query create a node with the header, it is deleted immediately
     * after creation by storing the header id from the file itself and deleting entities with that id title.
     */
    createUsers()
    createSkills()
    createInterests()
    createProjects()
    createOrganizations()
    createDistances()

    println("DATABASE BUILD COMPLETE \n")

    return
  }

  /**
   * Method that creates the user entities by loading information from the csv provided. The csv files must have their
   * appropriate names, however headers of the csv files do not need the appropriate titles as long as the format is the
   * same.
   */
  def createUsers():Unit = {
    val src = Source.fromFile("user.csv")
    val header = src.getLines().take(1).toList(0).toString()
    src.close()

    val x:String = header.split(',')(0)
    val file:String = fileLoc + "user.csv"
    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        CREATE(uu:UserNode {UID:line[0], FName:line[1], LName:line[2]})
      """).on("fileLocation" -> file).execute()
    Cypher(
      """
        MATCH (u:UserNode)
        WHERE u.UID = {header}
        DELETE u
      """).on("header" -> x).execute()

    return
  }

  /**
   * Method that creates the skill entities and relations between users and skills by loading information from the csv
   * provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createSkills():Unit = {
    val src = Source.fromFile("skill.csv")
    val header = src.getLines().take(1).toList(0).toString()
    src.close()

    val x:String = header.split(',')(0)
    val file:String = fileLoc + "skill.csv"

    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(ss:SkillNode {Name:line[1]})
        CREATE (u)-[r:SKILLED{Level:toFloat(line[2])}]->(ss)
      """).on("fileLocation" -> file).execute()
    Cypher(
      """
        MATCH (s:SkillNode)
        WHERE s.Name = {header}
        DELETE s
      """).on("header" -> x).execute()

    return
  }

  /**
   * Method that creates the interest entities and relations between users and interests by loading information from the
   * csv provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createInterests():Unit = {
    val src = Source.fromFile("interest.csv")
    val header = src.getLines().take(1).toList(0).toString()
    src.close()

    val x:String = header.split(',')(0)
    val file:String = fileLoc + "interest.csv"

    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(ee:InterestNode {Name:line[1]})
        CREATE (u)-[r:INTERESTED{Level:toFloat(line[2])}]->(ee)
      """).on("fileLocation" -> file).execute()
    Cypher(
      """"
        MATCH (i:InterestNode)
        WHERE i.Name = {header}
        DELETE i
      """).on("header" -> x).execute()

    return
  }

  /**
   * Method that creates the project entities and relations between users and projects by loading information from the
   * csv provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createProjects():Unit = {
    val src = Source.fromFile("project.csv")
    val header = src.getLines().take(1).toList(0).toString()
    src.close()

    val x:String = header.split(',')(0)
    val file:String = fileLoc + "project.csv"

    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(pp:ProjectNode {PName:line[1]})
        CREATE (u)-[r:WORKS_ON]->(pp)
      """).on("fileLocation" -> file).execute()
    Cypher(
      """"
        MATCH (p:ProjectNode)
        WHERE p.PName = {header}
        DELETE p
      """).on("header" -> x).execute()

    return
  }

  /**
   * Method that creates the organization entities and relations between users and organizations by loading information
   * from the csv provided. The csv files must have their appropriate names, however headers of the csv files do not
   * need the appropriate titles as long as the format is the same.
   */
  def createOrganizations():Unit = {
    val src = Source.fromFile("organization.csv")
    val header = src.getLines().take(1).toList(0).toString()
    src.close()

    val x:String = header.split(',')(0)
    val file:String = fileLoc + "organization.csv"

    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode)
        WHERE u.UID = line[0]
        MERGE(oo:OrganizationNode {OName:line[1], OType:line[2]})
        CREATE (u)-[r:BELONGS_TO]->(oo)
      """).on("fileLocation" -> file).execute()
    Cypher(
      """
        MATCH (o:OrganizationNode)
        WHERE o.OName = {header}
        DELETE o
      """).on("header" -> x).execute()

    return
  }

  /**
   * Method that creates the distance relations between organizations by loading information from the csv provided. The
   * csv files must have their appropriate names, however headers of the csv files do not need the appropriate titles as
   * long as the format is the same.
   */
  def createDistances():Unit = {
    val file:String = fileLoc + "distance.csv"

    Cypher(
      """
        LOAD CSV FROM {fileLocation} AS line
        MATCH (o1:OrganizationNode),(o2:OrganizationNode)
        WHERE o1.OName = line[0] AND o2.OName = line[1]
        CREATE (o1)-[r:DISTANCE_TO{Distance:toFloat(line[2])}]->(o2)
      """).on("fileLocation" -> file).execute()

    return
  }
}
