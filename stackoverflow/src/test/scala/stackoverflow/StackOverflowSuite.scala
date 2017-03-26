package stackoverflow

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 1
    override def kmeansKernels = 15
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("stackoverflow-test")
  lazy val sc: SparkContext = new SparkContext(conf)

  lazy val lines = sc.textFile(getClass.getResource("/stackoverflow/stackoverflow.csv").getPath)
  lazy val raw = testObject.rawPostings(lines)
  lazy val grouped = testObject.groupedPostings(raw)
  lazy val scored = testObject.scoredPostings(grouped)
  lazy val vectors = testObject.vectorPostings(scored)
  //    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

  lazy val means = testObject.kmeans(testObject.sampleVectors(vectors), vectors, debug = true)
  lazy val results = testObject.clusterResults(means, vectors)


  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("rawPostings") {
    raw.take(10).foreach(println)

    println(raw.toDebugString)

    assert(raw.count() === 8143801)
  }

  test("groupedPostings") {
    grouped.take(10).foreach(println)

    println(grouped.toDebugString)

    assert(grouped.count() === 2121822)
  }

  test("scoredPostings should work") {
    scored.take(10).foreach(println)

    println(scored.toDebugString)

    assert(scored.count() === 2121822)
    assert(scored.take(2)(1)._2 === 3)
  }

  test("vectorPostings should work") {
    vectors.take(10).foreach(println)

    println(vectors.toDebugString)

    assert(vectors.count() === 2121822)
    assert(vectors.take(2)(0)._1 === 4 * testObject.langSpread)
  }

  test("kmeans should work") {
    println("K-means: " + means.mkString(" | "))
    assert(means(0) === (1, 0))
  }

  /*test("results for java") {
    results(0)
    testObject.printResults(results)
  }*/

}