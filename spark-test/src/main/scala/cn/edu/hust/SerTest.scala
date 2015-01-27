package cn.edu.hust

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.WritableComparator
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.{ShuffledRDD, RDD}
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

object SerTest {
  private val ordering = implicitly[Ordering[Long]]

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Ser Cache Test").setMaster("local")
    val spark = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.FATAL)

    val slices = 4
    val n = 100 * slices
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
       var offset:Int = 0

       def join(key:Long,value:Double): Array[(Long,Double)] = {
         var currentKey:Long=0
         if((offset <= count)) {
           currentKey = WritableComparator.readLong(buf, offset)
           offset += 8
           val valueLength=WritableComparator.readLong(buf, offset)
           if (currentKey == key) {
             var j= 0
             var currentValue:Long=0
             val res = new Array[(Long, Double)](valueLength.toInt)
             while ((j<valueLength)&&(offset <= count)){
               offset += 8
               currentValue = WritableComparator.readLong(buf, offset)
               res(j)=((currentValue,value/valueLength))
               j += 1
             }
             offset+=8
             return res
           }
         }
         null
     }

  }

    def manuallyPageRank(lines: RDD[String], level: StorageLevel) {

      val iters = 2
      val links = lines.map { s =>
        val parts = s.split("\\s+")
        (parts(0).toLong, parts(1).toLong)
      }.groupByKey().
        asInstanceOf[ShuffledRDD[Long, _, _]].
        setKeyOrdering(ordering).
        asInstanceOf[RDD[(Long, Iterable[Long])]]

      var ranks = links.mapValues(v => 1.0)

      val cachedData = links.mapPartitions { iter =>
        val chunk = new KeyValueChunk(42960)
        val dos = new DataOutputStream(chunk)
        val (iter1, iter2) = iter.duplicate
      //  dos.writeLong(iter1.size)
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
          chunkData.offset=0
          res.iterator
        }
        else
          null
      }
        for (i <- 1 to iters) {
          val contribs = cachedData.zipPartitions(ranks)(zipPartitionsFunc)
          ranks = contribs.reduceByKey(_ + _).
            asInstanceOf[ShuffledRDD[Long, _, _]].
            setKeyOrdering(ordering).
            asInstanceOf[RDD[(Long, Double)]].mapValues(0.15 + 0.85 * _)
        }
        ranks.collect()
        val duration = System.currentTimeMillis - startTime
        println("Duration is " + duration / 1000.0 + " seconds")
      }
}
