/**
 * Title:       Collaborator.scala
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
 */

/**
 * Collaborator object that contains main method where it creates instances of the three classes that will be run to
 * simulate the three programs, DbLoader, QueryCollaborator, and QueryColOfCol.
 */
object Collaborator {

  /**
   * Main method that takes in command line arguments and creates instances of the different classes needed to simulate
   * the three programs.
   */
  def main(args: Array[String]): Unit = {
    println("Hello! this is the second project\n")
    println("Press enter to begin . . .")
    Console.readLine()

    /**
     * Creates an instance of DbLoader, passing the command line argument (where the file is located in local directory)
     * and runs a method within.
     */
    println(" ----------")
    println("| DbLoader |")
    println(" ----------\n")

    val myDbLoader = DbLoader
    myDbLoader.start()
    println()

    /**
     * Creates an instance of QueryCollaborator and runs a method within.
     */
    println(" -------------------")
    println("| QueryCollaborator |")
    println(" -------------------\n")

    val myQueryCollaborator = QueryCollaborator
    myQueryCollaborator.start()
    println()

    /**
     * Creates an instance of QueryColOfCol and runs a method within.
     */
    println(" ---------------")
    println("| QueryColOfCol |")
    println(" ---------------\n")

    val myQueryColOfCol = QueryColOfCol
    myQueryColOfCol.start()

    return
  }
}