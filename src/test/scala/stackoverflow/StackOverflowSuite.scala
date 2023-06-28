package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File
import scala.io.{Codec, Source}
import scala.util.Properties.isWin
import scala.concurrent.duration._

object StackOverflowSuite:
  val conf: SparkConf =
    new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)

  class StackOverflowSuite extends munit.FunSuite:
  import StackOverflowSuite._

  val fileLocation = "stackoverflow.csv"

  lazy val testObject = new StackOverflow {
    override val languages =
      List(
        "JavaScript",
        "Java",
        "PHP",
        "Python",
        "C#",
        "C++",
        "Ruby",
        "CSS",
        "Objective-C",
        "Perl",
        "Scala",
        "Haskell",
        "MATLAB",
        "Clojure",
        "Groovy"
      )
    override def getLangSpread = 50000
    override def kMeansKernels = 45
    override def kMeansEta: Double = 20.0d
    override def kMeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable =
      try {
        testObject
        true
      } catch {
        case _: Throwable => false
      }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("Data can be loaded from the file") {
    val lines = testObject.readLinesAsStream(sc, fileLocation)
    assert(lines != null)
    assert(lines.nonEmpty)
  }

  test("Posts can be extracted from the lines") {
    val lines = testObject.readLinesAsStream(sc, fileLocation)
    val posts = testObject.extractRawPostings(lines)
    assert(posts != null)
    assert(posts.count() > 0)
  }

  test("Can group questions and answers together") {
    val lines = testObject.readLinesAsStream(sc, fileLocation)
    val posts = testObject.extractRawPostings(lines)
    val groupedPostings = testObject.groupPostings(posts)
    assert(groupedPostings != null)
    assert(groupedPostings.count() > 0)
  }

  test("Can compute max score for each post") {
    val lines = testObject.readLinesAsStream(sc, fileLocation)
    val posts = testObject.extractRawPostings(lines)
    val groupedPostings = testObject.groupPostings(posts)
    val maxScores = testObject.scorePostings(groupedPostings)
    assert(maxScores != null)
    assert(maxScores.count() > 0)
  }

  test("Can compute vectors for k-means") {
    val lines = testObject.readLinesAsStream(sc, fileLocation)
    val posts = testObject.extractRawPostings(lines)
    val groupedPostings = testObject.groupPostings(posts)
    val maxScores = testObject.scorePostings(groupedPostings)
    val vectors = testObject.vectorizePostings(maxScores)
    assert(vectors != null)
    assert(vectors.count() > 0)
  }

  override val munitTimeout: Duration = 300.seconds
  }
