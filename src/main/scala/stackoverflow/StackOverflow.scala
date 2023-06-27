package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
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

case class Posting(
                    postingType: Int,
                    id: Int,
                    acceptedAnswer: Option[Int],
                    parentId: Option[QID],
                    score: Int,
                    tags: Option[String]
                  ) extends Serializable

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

object StackOverflow extends StackOverflow {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  @transient lazy val sparkConf: SparkConf =
    new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  @transient lazy val sparkContext: SparkContext = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    val inputFileLocation: String = "stackoverflow.csv"

    val lines = readLinesAsStream(sparkContext, inputFileLocation)
    val rawPostings = extractRawPostings(lines)
    val groupedPostings = groupPostings(rawPostings)
    val scoredPostings = scorePostings(groupedPostings)
    val vectorizedPostings = vectorizePostings(scoredPostings)
    assert(
      vectorizedPostings.count() == 1042132,
      "Incorrect number of vectors: " + vectorizedPostings.count()
    )

    val means = runKMeans(sampleVectors(vectorizedPostings, kMeansKernels * languages.length), vectorizedPostings, debug = true)
    val results = calculateClusterResults(means, vectorizedPostings)
    printResults(results)

    val kValues = Array(10, 20, 30) // Specify the desired values of k here

    kValues.foreach { k =>
      val means = runKMeans(sampleVectors(vectorizedPostings, k * languages.length), vectorizedPostings, k, debug = true)
      val results = calculateClusterResults(means, vectorizedPostings)
      println(s"\nResults for k=$k:")
      printResults(results)
    }
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

  def readLinesAsStream(sparkContext: SparkContext, inputFileLocation: String): RDD[String] = {
    val fileData = Source.fromResource(inputFileLocation)(Codec.UTF8)
    sparkContext.parallelize(fileData.getLines().toList)
  }

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

  def groupPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(_.postingType == 1).map(post => (post.id, post))
    val answers = postings.filter(_.postingType == 2).map(post => (post.parentId.get, post))
    (questions join answers).groupByKey
  }

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

  def converged(distance: Double): Boolean =
    distance < kMeansEta

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

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("CLUSTER INFORMATION:")
    println("  Score  Language Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results) {
      println(f"${score}%7d  ${lang}%-17s ${size}%7d")
    }
  }
}