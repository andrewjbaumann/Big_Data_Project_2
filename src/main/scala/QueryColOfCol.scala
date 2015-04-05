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

/*
  Tony: 
        List One:
          c, potatoes
          c, corn
          e, corn
        List Two (input):
          find 
          potatoes
          tomatoes
          
          Returns only C
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
  implicit val connection = Neo4jREST()
  //implicit val connection = Neo4jREST("localhost", 7474, "/db/data/")
  private var user:String = ""
  private var particularInterests:List[String] = List()


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
    var valid:Boolean = false
    var response:String = ""
    var more:Boolean = false
    var moreResponse:String = ""

    /**
     * Asks user whether they want to query the database or not
     */
    print("(Y/y) to query database for collaborators of collaborators with similar interests: ")
    response = Console.readLine()

    /**
     * Boolean check for a confirmation of continuing
     */
    if(response == "y" || response == "Y") {
      valid = true
    }

    /**
     * If Boolean check is passed, it continues with the query.
     */
    if(valid == true) {
      print("Enter user id: ")
      user = Console.readLine()

      print("Enter a particular interest for the col of col: ")
      particularInterests = particularInterests.:+(Console.readLine())

      print("More interests to look for? ('Y'/'y'): ")
      moreResponse = Console.readLine()

      if(moreResponse == "y" || moreResponse == "Y") {
        more = true
      }

      while(more == true) {
        print("Enter a particular interest for the col of col: ")
        particularInterests = particularInterests.:+(Console.readLine())

        print("More interests to look for? ('Y'/'y'): ")
        moreResponse = Console.readLine()

        if(moreResponse == "y" || moreResponse == "Y") {
          more = true
        }
        else {
          more = false
        }
      }

      val comm = Cypher(
        """
          START user = node(*)
          MATCH (user:UserNode), (col:UserNode), (colOfCol:UserNode), (p1:ProjectNode), (p2:ProjectNode), (i:InterestNode)
          WHERE (user.UID = {x}) AND (user<>col) AND ((user)-->(p1)<--(col)-->(p2)<--(colOfCol)) AND (colOfCol-->i)
          RETURN colOfCol.UID as id, i.IName as interests
        """).on("x" -> user)

      val commStream = comm()

      /**
       * Prints out a mapped list of returned values from the cypher query.
       */
      var myList = (commStream.map(row => {row[String]("id")->row[String]("interests")}).toList)
      println(myList)

      myList.contains('C','potatoes')
      /*for(x<-myList) {
        for(y<-particularInterests){
          if(x._2==y){
            println(x + " CONTAINS " + y)
          }
          else {
            println(x+ " DOESNT CONTAIN " + y)
          }
        }
      }
      */

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
