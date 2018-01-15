package postgapspark

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scopt.OptionParser

case class Config(in: String = "", out: String = "",
                  cores: String = "*", eco: String = "",
                  kwargs: Map[String,String] = Map())

case class PGLine(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object PostgapQC {
  val progVersion = "0.5"
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

    val ecoLT = PostgapECO.generateLookupTable(ss, config.eco)

    // read the data with a predefined schema
    // adding 2 more columns vep_max_score and fg_score
    val pgd = ss.read
      .format("csv")
      .option("header", "true")
      // .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .schema(PostgapData.Schema)
      .load(config.in)

    // TODO use udf instead this messy caos
    val maxVEP = udf((vep: String) => PostgapECO.computeMaxVEP(vep, ecoLT))
    val pgdWithVepMax = pgd.withColumn("vep_max_score",
        when($"vep_terms".isNotNull, maxVEP($"vep_terms"))
          .otherwise(0))
      .toDF()

    val fgScore = udf((gtex: Double, fantom5: Double, dhs: Double, pchic: Double) =>
      PostgapFG.computeFGScore(gtex, fantom5, dhs, pchic))

    val pgdWithFG = pgdWithVepMax.withColumn("fg_score",
        when($"GTEx".isNotNull and $"Fantom5".isNotNull and $"DHS".isNotNull and $"PCHiC".isNotNull,
          fgScore($"GTEx", $"Fantom5", $"DHS", $"PCHiC")).otherwise(0))
      .toDF()

    // print schema and create a temp table to query
    // pgd.printSchema()

    pgdWithFG.createOrReplaceTempView("postgap")

    // persist the created table
    ss.table("postgap").persist(StorageLevel.MEMORY_AND_DISK)

    // get filterout lines without the proper score levels at func genomics
    val filteredOTData = ss.sql("SELECT * FROM postgap WHERE vep_max_score >= 0.65 or fg_score > 0 or nearest = 1")
    filteredOTData.write.format("csv").option("header", "true").option("delimiter", "\t").save(config.out)

    val filteredOTDataCount = filteredOTData.count
    println(s"The number of rows with (vep >= 0.65 or fg > 0 or nearest) is $filteredOTDataCount")

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

  val parser = new OptionParser[Config](progName) {
    head(progName, progVersion)

    opt[String]('c', "cores")
      .valueName("<num-cores|*>")
      .action( (x, c) => c.copy(in = x) )
      .text("num cores to run locally default '*'")

    opt[String]('i', "in").required()
      .valueName("<file>")
      .action( (x, c) => c.copy(in = x) )
      .text("in filename")

    opt[String]('e', "eco").required()
      .valueName("<file>")
      .action( (x, c) => c.copy(in = x) )
      .text("eco filename")

    opt[String]('o', "out").required()
      .valueName("<folder>")
      .action( (x, c) => c.copy(out = x) )
      .text("out folder to save computed rdd partitions")

    opt[Map[String,String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action( (x, c) => c.copy(kwargs = x) )
      .text("other arguments")

    note("You need to specify eco file input file and output dir.\n")
  }
}
