package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.WritableComparator
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.collection.mutable.ArrayBuffer



/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */


class FloatChunk(size: Int = 4196) extends ByteArrayOutputStream(size) {
  def max(): Float = {
    var maxValue = 0.0f
    var currentValue = 0.0f
    var offset = 0
    while (offset <= count) {
      currentValue = WritableComparator.readFloat(buf, offset)
      if (currentValue > maxValue) {
        maxValue = currentValue
      }
      offset += 4
    }
    maxValue
  }
}

case class FloatWrapper(value: Float)

object SerTest {
  def testMemory(input: RDD[FloatWrapper]) {
    testNative(input, StorageLevel.MEMORY_ONLY)
  }

  def testMemorySer(input: RDD[FloatWrapper]) {
    testNative(input, StorageLevel.MEMORY_ONLY_SER)
  }

  def testNative(input: RDD[FloatWrapper], level: StorageLevel) {
    println("-------- Native " + level.description + " --------")

    val cachedData = input.persist(level)
    
    val seqMax = (x: Float, y: FloatWrapper) => if (y.value > x) y.value else x
    val combMax = (x: Float, y: Float) => if (y > x) y else x

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.aggregate(0.0f)(seqMax, combMax))
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.aggregate(0.0f)(seqMax, combMax)
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + " seconds")
    }

    cachedData.unpersist()
    println()
  }

  def testManuallyOptimized(input: RDD[FloatWrapper]) {
    println("------------------ Manually optimized ------------------")

    val cachedData = input.mapPartitions { iter =>
      val chunk = new FloatChunk(41960)
      val dos = new DataOutputStream(chunk)
      iter.foreach(x => dos.writeFloat(x.value))
      Iterator(chunk)
    }.persist(StorageLevel.MEMORY_ONLY)

    var startTime = System.currentTimeMillis
    println("Max value is " + cachedData.map(_.max()).max())
    var duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

    for (i <- 1 to 5) {
      startTime = System.currentTimeMillis
      cachedData.map(_.max()).max()
      duration = System.currentTimeMillis - startTime
      println("Duration is " + duration / 1000.0 + " seconds")
    }

    cachedData.unpersist()
    println()
  }

 /*
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Ser Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n = 6000000 * slices
    val rawData = spark.parallelize(1 to n, slices).map(x => new FloatWrapper(x.toFloat))

    testManuallyOptimized(rawData)
    //testMemorySer(rawData)
    //testMemory(rawData)

    spark.stop()
  }
  */

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Ser Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n = 600 * slices
    val random =new Random()
    val num=slices//random.nextLong()
    val rawData = spark.parallelize(0 to n-1, slices).map(x =>
      x.toString()+"\t"+((x+num)%n).toString()+"\t")
    //originallyPageRank(rawData,StorageLevel.MEMORY_AND_DISK_SER)

    //originallyPageRank(rawData,StorageLevel.MEMORY_ONLY)

    //originallyPageRank(rawData,StorageLevel.MEMORY_ONLY_SER)

    manuallyPageRank(rawData,StorageLevel.MEMORY_ONLY)

    spark.stop()
  }


  def originallyPageRank(lines: RDD[String], level: StorageLevel) {

    val startTime = System.currentTimeMillis

    val iters = 1
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.groupByKey().persist(level)

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.collect()

    val duration = System.currentTimeMillis - startTime
    println("Duration is " + duration / 1000.0 + " seconds")

  }



  class KeyValueChunk(size: Int = 4296) extends ByteArrayOutputStream(size) {

    def join(key:Long,value:Double): ArrayBuffer[(Long,Double)] = {
      val res = new ArrayBuffer[(Long, Double)]
      var i = 0
      var offset:Int = 0
      val length = WritableComparator.readLong(buf,offset)
      var currentKey:Long=0
      while ((i<length)&&(offset <= count)) {
        offset += 8
        currentKey = WritableComparator.readLong(buf, offset)
        offset += 8
        val valueLength=WritableComparator.readLong(buf, offset)

        if (currentKey == key) {
          var j= 0
          var currentValue:Long=0
          while ((j<valueLength)&&(offset <= count)){
            offset += 8
            currentValue = WritableComparator.readLong(buf, offset)
            res += ((currentValue,value/valueLength))
            j += 1
          }
          return res
        }
        offset += 8*valueLength.toInt
        i+=1
      }
      null
    }
  }

    def manuallyPageRank(lines: RDD[String], level: StorageLevel) {

      val iters = 1
      val links = lines.map { s =>
        val parts = s.split("\\s+")
        (parts(0).toLong, parts(1).toLong)
      }.groupByKey()

      var ranks = links.mapValues(v => 1.0)

      val cachedData = links.mapPartitions { iter =>
        val chunk = new KeyValueChunk(42960)
        val dos = new DataOutputStream(chunk)
        val (iter1, iter2) = iter.duplicate
        dos.writeLong(iter1.size)
        iter2.foreach(x => {
          dos.writeLong(x._1)
          dos.writeLong(x._2.size)
          x._2.foreach(y => dos.writeLong(y))
        })
        Iterator(chunk)
      }.persist(StorageLevel.MEMORY_ONLY)

      val startTime = System.currentTimeMillis

      def zipPartitionsFunc(l: Iterator[KeyValueChunk], r: Iterator[(Long, Double)]): Iterator[(Long, Double)] = {
        val res = new ArrayBuffer[(Long, Double)]
        // exercise for the reader: suck either l or r into a container before iterating
        // and consume it in random order to achieve more random pairing if desired
        if (l.hasNext) {
          val chunkData = l.next()
          while (r.hasNext) {
            val keyValue = r.next()
            res ++= chunkData.join(keyValue._1, keyValue._2)
          }
          res.iterator
        }
        else
          null
      }
        for (i <- 1 to iters) {
          val contribs = cachedData.zipPartitions(ranks)(zipPartitionsFunc)
          ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
        }
        ranks.collect()
        val duration = System.currentTimeMillis - startTime
        println("Duration is " + duration / 1000.0 + " seconds")
      }
}
