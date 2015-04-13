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
 *              To run:     scala Collaborator
 * Notes:       Completed(✓) - Tony
 */

import scala.actors.Actor
import scala.actors.Actor._
import scala.io.Source
import org.anormcypher._

/**
 * DbLoader class that takes in a string input (file location of csvs) and runs queries on the neo4j database, creating
 * entities and relations between them.
 */
object DbLoader {

  /**
   * Connects to the neo4j database (version run on my machine does not require authentication, whereas other versions
   * may require a different setup).
   */
  //implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")
  implicit val connection = Neo4jREST("localhost", 7474, "/db/data/", "neo4j", "Neo4j")
  private var fileLoc = ""

  /**
   * Method that starts by clearing the database of any relations, entities tied to relations, and lone entities for
   * testing.
   */
  def start():Unit = {

    val getCurrentDirectory:String = new java.io.File( "." ).getCanonicalPath
    fileLoc = "file:///" + getCurrentDirectory + "/"

    println("CLEARING DATABASE . . .")

    Cypher(
      """
        MATCH n-[r]->m
        DELETE r, m, n
      """).execute()
    Cypher(
      """
        MATCH n
        DELETE n
      """).execute()

    println("DATABASE CLEARED (✔)")

    println("CREATING INDEXES . . .")

    Cypher(
      """
        CREATE INDEX ON :UserNode(UID)
      """).execute()
    Cypher(
      """
        CREATE INDEX ON :SkillNode(Name)
      """).execute()
    Cypher(
      """
        CREATE INDEX ON :InterestNode(Name)
      """).execute()
    Cypher(
      """
        CREATE INDEX ON :ProjectNode(PName)
      """).execute()
    Cypher(
      """
        CREATE INDEX ON :OrganizationNode(OName)
      """).execute()

    println("INDEXES CREATED (✔)")

    println("BUILDING DATABASE . . .")

    /**
     * Calls the different methods within this class to run the different queries on the database. In the methods that
     * involve creating entities, since the LOAD CSV query create a node with the header, it is deleted immediately
     * after creation by storing the header id from the file itself and deleting entities with that id title.
     */
    createUsers()

    /**
     * After creating the users, these 4 entity types can be created without dependencies on each other and can be done
     * concurrently with actors.
     */
    val skillsActor = actor {
      createSkills()
    }

    val interestsActor = actor {
      createInterests()
    }

    val projectsActor = actor {
      createProjects()
    }

    val organizationsActor = actor {
      createOrganizations()
    }

    /**
     * After the organization entities have been created, the distances between the organizations can be created by
     * listening for when the organization actor terminates.
     */
    while(organizationsActor.getState != Actor.State.Terminated) {

    }

    createDistances()

    while(skillsActor.getState != Actor.State.Terminated && interestsActor.getState != Actor.State.Terminated && projectsActor.getState != Actor.State.Terminated && organizationsActor.getState != Actor.State.Terminated) {

    }

    println("DATABASE BUILD COMPLETE (✔)")

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
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        CREATE (uu:UserNode{UID:line[0], FName:line[1], LName:line[2]})
      """).on("fileLocation" -> file).execute()
    Cypher(
      """
        MATCH (u:UserNode{UID:{header}})
        DELETE u
      """).on("header" -> x).execute()

    println("\tUser Entities (✔)")

    return
  }

  /**
   * Method that creates the skill entities and relations between users and skills by loading information from the csv
   * provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createSkills():Unit = {
    val file:String = fileLoc + "skill.csv"

    Cypher(
      """
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode{UID:line[0]})
        MERGE (ss:SkillNode{Name:line[1]})
        CREATE (u)-[r:SKILLED{Level:toFloat(line[2])}]->(ss)
      """).on("fileLocation" -> file).execute()

    println("\tSkill Entities (✔)")

    return
  }

  /**
   * Method that creates the interest entities and relations between users and interests by loading information from the
   * csv provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createInterests():Unit = {
    val file:String = fileLoc + "interest.csv"

    Cypher(
      """
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode{UID:line[0]})
        MERGE (ee:InterestNode{Name:line[1]})
        CREATE (u)-[r:INTERESTED{Level:toFloat(line[2])}]->(ee)
      """).on("fileLocation" -> file).execute()

    println("\tInterest Entities (✔)")

    return
  }

  /**
   * Method that creates the project entities and relations between users and projects by loading information from the
   * csv provided. The csv files must have their appropriate names, however headers of the csv files do not need the
   * appropriate titles as long as the format is the same.
   */
  def createProjects():Unit = {
    val file:String = fileLoc + "project.csv"

    Cypher(
      """
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode{UID:line[0]})
        MERGE (pp:ProjectNode{PName:line[1]})
        CREATE (u)-[r:WORKS_ON]->(pp)
      """).on("fileLocation" -> file).execute()

    println("\tProject Entities (✔)")

    return
  }

  /**
   * Method that creates the organization entities and relations between users and organizations by loading information
   * from the csv provided. The csv files must have their appropriate names, however headers of the csv files do not
   * need the appropriate titles as long as the format is the same.
   */
  def createOrganizations():Unit = {
    val file:String = fileLoc + "organization.csv"

    Cypher(
      """
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        MATCH (u:UserNode{UID:line[0]})
        MERGE (oo:OrganizationNode{OName:line[1], OType:line[2]})
        CREATE (u)-[r:BELONGS_TO]->(oo)
      """).on("fileLocation" -> file).execute()

    println("\tOrganization Entities (✔)")

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
        USING PERIODIC COMMIT 10000
        LOAD CSV FROM {fileLocation} AS line
        MATCH (o1:OrganizationNode{OName:line[0]}), (o2:OrganizationNode{OName:line[1]})
        CREATE (o1)-[r:DISTANCE_TO{Distance:toFloat(line[2])}]->(o2)
      """).on("fileLocation" -> file).execute()

    println("\tDistance Relations (✔)")

    return
  }
}
