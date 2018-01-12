package postgapspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class PGLine(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object PostgapQC {

  // val conf: SparkConf = new SparkConf().setAppName("PostgapQC").setMaster("local[*]")
  val conf: SparkConf = new SparkConf()
  val sc: SparkSession = SparkSession.builder()
    .appName("PostgapQC").master("local[*]").config(conf).getOrCreate()

  val pgRdd: RDD[PGLine] =
    sc.textFile(PostgapData.filePath)
      .map(d => PostgapData.parse(d)).persist(StorageLevel.MEMORY_AND_DISK)

  def main(args: Array[String]) {
    /* Languages ranked according to (1) */
    sc.stop()
  }
}
