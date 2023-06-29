package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import math.Fractional.Implicits.infixFractionalOps
import math.Integral.Implicits.infixIntegralOps
import math.Numeric.Implicits.infixNumericOps
import org.apache.log4j.{Logger, Level}

import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Properties.isWin
import scala.io.Source
import scala.io.Codec

object Aliases {
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
}
import Aliases._

// Define the Posting case class to represent a Stack Overflow posting
case class Posting(
                    postingType: Int,
                    id: Int,
                    acceptedAnswer: Option[Int],
                    parentId: Option[QID],
                    score: Int,
                    tags: Option[String]
                  ) extends Serializable

// Define the StackOverflowInterface trait with methods and properties for Stack Overflow functionality
trait StackOverflowInterface {
  def calculateClusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)]
  def groupPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])]
  def runKMeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iterations: Int = 1, debug: Boolean = false): Array[(Int, Int)]
  def extractRawPostings(lines: RDD[String]): RDD[Posting]
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)], k: Int): Array[(Int, Int)] // Updated method signature
  def scorePostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)]
  def vectorizePostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)]
  def getLangSpread: Int
  val languages: List[String]
}

// Implement the StackOverflow class that extends the StackOverflowInterface
object StackOverflow extends StackOverflow {

  // Set logger level to ERROR
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  // Create Spark configuration and Spark context
  @transient lazy val sparkConf: SparkConf =
    new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  @transient lazy val sparkContext: SparkContext = new SparkContext(sparkConf)


  def main(args: Array[String]): Unit = {
    val inputFileLocation: String = "stackoverflow.csv"

    // Read lines from the input file as a stream
    val lines = readLinesAsStream(sparkContext, inputFileLocation)
    // Extract raw postings from the lines
    val rawPostings = extractRawPostings(lines)
    // Group postings by a common identifier (QID)
    val groupedPostings = groupPostings(rawPostings)
    // Score the postings based on their attributes
    val scoredPostings = scorePostings(groupedPostings)
    // Vectorize the scored postings
    val vectorizedPostings = vectorizePostings(scoredPostings)
    // Assert the count of vectorized postings
    assert(
      vectorizedPostings.count() == 1042132,
      "Incorrect number of vectors: " + vectorizedPostings.count()
    )

    // Run K-means clustering on the vectorized postings
    val means = runKMeans(sampleVectors(vectorizedPostings, kMeansKernels * languages.length), vectorizedPostings, debug = true)
    // Calculate cluster results based on the means and vectorized postings
    val results = calculateClusterResults(means, vectorizedPostings)
    // Print the results of the clustering
    printResults(results)

    val kValues = Array(25, 30, 35, 40, 45, 50, 55) // Specify the desired values of k here

    // Iterate over the specified k values
    kValues.foreach { k =>
      // Run K-means clustering with the current k
      val means = runKMeans(sampleVectors(vectorizedPostings, k * languages.length), vectorizedPostings, k, debug = true)
      val sse = calculateSSE(means, vectorizedPostings)
      val results = calculateClusterResults(means, vectorizedPostings)
      println(s"\nSum of squared error=$sse:")
      println(s"\nResults for k=$k:")
      printResults(results)
    }

    // Calculate SSE values for each k
    val sseValues = kValues.map { k =>
      val means = runKMeans(sampleVectors(vectorizedPostings, k * languages.length), vectorizedPostings)
      val sse = calculateSSE(means, vectorizedPostings)
      (k, sse)
    }
    // Find the optimal k based on the elbow method
    val optimalK = findOptimalK(sseValues)

    println(s"Optimal k value: $optimalK")

    // Run K-means with the optimal k
    val optimalMeans = runKMeans(sampleVectors(vectorizedPostings, optimalK * languages.length), vectorizedPostings, optimalK)
    val resultsOptimal = calculateClusterResults(optimalMeans, vectorizedPostings)
    println(s"\nResults for k=$optimalK:")
    printResults(resultsOptimal)

  }

  // Calculate the sum of squared error (SSE) for a set of means and vectors
  def calculateSSE(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Double = {
    vectors
      .map(v => (findClosest(v, means), v))
      .map { case (closest, v) =>
        calculateEuclideanDistance(v, closest)
      }
      .sum()
  }

  // Find the optimal k based on SSE values using the elbow method
  def findOptimalK(sseValues: Array[(Int, Double)]): Int = {
    val sseDifferences = sseValues.sliding(2).map { case Array(a, b) =>
      val (_, sseA) = a
      val (k, sseB) = b
      (k, sseB - sseA)
    }.toArray

    val maxSSEDifferenceIndex = sseDifferences.indexWhere { case (_, diff) =>
      diff < sseDifferences.maxBy(_._2)._2 * 0.1
    }

    val optimalK = if (maxSSEDifferenceIndex != -1) {
      val (k, _) = sseDifferences(maxSSEDifferenceIndex)
      k
    } else {
      val (k, _) = sseValues.maxBy { case (_, sse) => sse }
      k
    }
    optimalK
  }
}

class StackOverflow extends StackOverflowInterface with Serializable {

  val languages =
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

  def getLangSpread: Int = 50000
  assert(
    getLangSpread > 0,
    "If langSpread is zero, we can't recover the language from the input data!"
  )

  def kMeansKernels: Int = 45
  def kMeansEta: Double = 20.0d
  def kMeansMaxIterations: Int = 120

  //Reads the lines from an input file location into an RDD of strings.
  def readLinesAsStream(sparkContext: SparkContext, inputFileLocation: String): RDD[String] = {
    val fileData = Source.fromResource(inputFileLocation)(Codec.UTF8)
    sparkContext.parallelize(fileData.getLines().toList)
  }

  //Extracts the raw postings from RDD[String] and maps them to Posting objects.
  def extractRawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(
        postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None
      )
    })

  // Groups the postings by question ID and returns RDD[(QID, Iterable[(Question, Answer)])].
  def groupPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(_.postingType == 1).map(post => (post.id, post))
    val answers = postings.filter(_.postingType == 2).map(post => (post.parentId.get, post))
    (questions join answers).groupByKey
  }

  //Computes the high score for each question-answer pair and returns RDD[(Question, HighScore)].
  def scorePostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
    def answerHighScore(answers: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < answers.length) {
        val score = answers(i).score
        if (score > highScore) highScore = score
        i += 1
      }
      highScore
    }

    grouped
      .mapValues(iterable => (iterable.head._1, answerHighScore(iterable.map(_._2).toArray)))
      .values
  }

  //Maps the scored postings to a vector representation (LangIndex, HighScore).
  def vectorizePostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    def firstLangInTag(tag: Option[String], languages: List[String]): Option[Int] =
      tag match {
        case None => None
        case Some(lang) =>
          val index = languages.indexOf(lang)
          if (index >= 0) Some(index) else None
      }

    scored
      .map { case (question, highScore) =>
        (firstLangInTag(question.tags, languages), highScore)
      }
      .filter(_._1.isDefined)
      .map { case (langIndex, highScore) =>
        (langIndex.get * getLangSpread, highScore)
      }
      .persist
  }

  // Samples vectors for clustering based on a given number k and language distribution.
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)], k: Int): Array[(Int, Int)] = {
    assert(
      k % languages.length == 0,
      "k should be a multiple of the number of languages studied."
    )
    val perLang = k / languages.length

    val res = vectors
      .groupByKey()
      .flatMap { case (lang, vectors) =>
        val langVectors = vectors.toArray
        val langSize = langVectors.length
        val rnd = new util.Random(lang)

        if (langSize >= perLang) {
          // Randomly select perLang vectors
          val sampledVectors = rnd.shuffle(langVectors.toList).take(perLang)
          sampledVectors.map((lang, _))
        } else {
          // Duplicate vectors to meet the required perLang count
          val duplicatedVectors = Seq.fill(perLang)(langVectors(rnd.nextInt(langSize)))
          duplicatedVectors.map((lang, _))
        }
      }
      .collect()

    assert(res.length == k, res.length)
    res
  }


  // Performs the K-means clustering algorithm using an iterative approach.
  // It takes an initial set of means, vectors, and optional parameters like iterations and debug mode.
  // It returns the updated means after convergence.
  @tailrec
  final def runKMeans(
                       means: Array[(Int, Int)],
                       vectors: RDD[(Int, Int)],
                       iterations: Int = 1,
                       debug: Boolean = false
                     ): Array[(Int, Int)] = {
    val newMeansMap: scala.collection.Map[(Int, Int), (Int, Int)] =
      vectors
        .map(v => (findClosest(v, means), v))
        .groupByKey
        .mapValues(averageVectors)
        .collectAsMap

    val newMeans: Array[(Int, Int)] = means.map(oldMean => newMeansMap(oldMean))
    val distance = calculateEuclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iterations
                 |  * current distance: $distance
                 |  * desired distance: $kMeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kMeansKernels) {
        println(
          f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
            f"  distance: ${calculateEuclideanDistance(means(idx), newMeans(idx))}%8.0f"
        )
      }
    }

    if (converged(distance)) newMeans
    else if (iterations < kMeansMaxIterations) {
      runKMeans(newMeans, vectors, iterations + 1, debug)
    } else {
      if (debug) println("Reached max iterations!")
      newMeans
    }
  }

  //Checks if the distance between means is below the convergence threshold.
  def converged(distance: Double): Boolean =
    distance < kMeansEta

  //Calculates the Euclidean distance between two vectors or arrays of vectors.
  def calculateEuclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }


  def calculateEuclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += calculateEuclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  //Finds the closest mean to a given vector.
  def findClosest(point: (Int, Int), centers: Array[(Int, Int)]): (Int, Int) = {
    var bestCenter: (Int, Int) = null
    var closest = Double.PositiveInfinity
    for (center <- centers) {
      val tempDistance = calculateEuclideanDistance(point, center)
      if (tempDistance < closest) {
        closest = tempDistance
        bestCenter = center
      }
    }
    bestCenter
  }

  //Calculates the average vector for a set of points.
  def averageVectors(points: Iterable[(Int, Int)]): (Int, Int) = {
    val iterator = points.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iterator.hasNext) {
      val item = iterator.next()
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //Calculates cluster information such as the most common language, percentage, cluster size, and
  // median score for each cluster.
  def calculateClusterResults(
                               means: Array[(Int, Int)],
                               vectors: RDD[(LangIndex, HighScore)]
                             ): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val mostCommonLang = vs
        .groupBy(_._1)
        .view
        .mapValues(_.size)
        .toMap
        .maxBy(_._2)
      val langLabel: String = languages(mostCommonLang._1 / getLangSpread)
      val langPercent: Double =
        mostCommonLang._2 * 100 / vs.size
      val clusterSize: Int = vs.size
      val (xs, ys) = vs.map(_._2).toArray.sorted.splitAt(vs.size / 2)
      val medianScore: Int =
        if (xs.length == ys.length) (xs.last + ys.head) / 2 else ys.head

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)(Ordering.Int.reverse)
  }

  // Prints the cluster information in a formatted manner.
  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("CLUSTER INFORMATION:")
    println("  Score  Language Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results) {
      println(f"${score}%7d  ${lang}%-17s ${size}%7d")
    }
  }
}