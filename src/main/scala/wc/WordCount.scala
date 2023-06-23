package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object WordCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    // Read input file and create an RDD of lines
    val lines = sc.textFile(args(0))

    // Filter lines and count the number of incoming edges for each user ID divisible by 100.
    val counts = lines
      .filter(line => {
        val fields = line.split(",")
        // Check if line has at least two fields and the second field is divisible by 100
        fields.length >= 2 && fields(1).toInt % 100 == 0
      })
      .map(line => {
        // Split line into fields
        val fields = line.split(",")
        // Create a key-value pair with the user ID as the key and 1 as the value
        (fields(1), 1)
      })
      // Reduce by key to get the count of incoming edges for each user ID
      .reduceByKey(_ + _)

    // Log the lineage information
    val lineageInfo = counts.toDebugString
    logger.info(lineageInfo)

    // Save the results as text files
    counts.saveAsTextFile(args(1))
    sc.stop()
  }
}
