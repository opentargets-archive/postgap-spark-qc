package postgapspark

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

case class Config(in: String = "", out: String = "",
                  cores: String = "*", kwargs: Map[String,String] = Map())

case class PGLine(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object PostgapQC {
  val progVersion = "0.1"
  val progName = "PostgapQC"

  def runQC(config: Config): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .setAppName("PostgapQC")
      .setMaster(s"local[${config.cores}]")

    val ss: SparkSession = SparkSession.builder
      .config(conf)
      .getOrCreate

    // needed to use the $notation
    import ss.implicits._

    val pgd = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(config.in)

    // print schema and create a temp table to query
    pgd.printSchema()
    pgd.createOrReplaceTempView("postgap")

    // persist the created table
    ss.table("postgap").persist(StorageLevel.MEMORY_AND_DISK)
    val gwasSNPs = ss.sql("SELECT * from postgap WHERE ls_snp_is_gwas_snp = 1")
    gwasSNPs.sample(true, 0.2).show()
    ss.sql("SELECT count(*) from postgap WHERE ls_snp_is_gwas_snp = 1").show()

    // pgd.sample(true, 0.1).show()
    // pgd.write.format("csv").option("header", "true").option("delimiter", "\t").save(config.out)
    gwasSNPs.write.format("csv").option("header", "true").option("delimiter", "\t").save(config.out)

    // pgd.rdd.saveAsTextFile(config.out)

    ss
  }

  def main(args: Array[String]) {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        runQC(config).stop
      case None =>
    }
  }

  val parser = new scopt.OptionParser[Config](progName) {
    head(progName, progVersion)

    opt[String]('c', "cores")
      .valueName("<num-cores|*>")
      .action( (x, c) => c.copy(in = x) )
      .text("num cores to run locally default '*'")

    opt[String]('i', "in").required()
      .valueName("<file>")
      .action( (x, c) => c.copy(in = x) )
      .text("in filename")

    opt[String]('o', "out").required()
      .valueName("<folder>")
      .action( (x, c) => c.copy(out = x) )
      .text("out folder to save computed rdd partitions")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    note("If missing --in <file> internal package sample will be used instead.\n")
  }
}
