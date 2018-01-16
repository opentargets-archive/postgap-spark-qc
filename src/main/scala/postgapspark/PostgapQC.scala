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

    val funcDist = udf((snpChr: String, geneChr: String, snpPos: Int, genePos: Int) =>
      PostgapData.computeAbsDist(snpChr, geneChr, snpPos, genePos))

    val pgdWithDist = pgdWithFG.withColumn("snp_gene_dist",
      when($"GRCh38_chrom".isNotNull and $"GRCh38_gene_chrom".isNotNull
          and $"GRCh38_pos".isNotNull and $"GRCh38_gene_pos".isNotNull,
        funcDist($"GRCh38_chrom", $"GRCh38_gene_chrom", $"GRCh38_pos", $"GRCh38_gene_pos")).otherwise(Int.MaxValue))
      .toDF()
    // print schema and create a temp table to query
    // pgd.printSchema()

    pgdWithDist.createOrReplaceTempView("postgap")

    // persist the created table
    ss.table("postgap").persist(StorageLevel.MEMORY_AND_DISK)

    val aggregateBySource = ss.sql("""
      SELECT gwas_source, count(*)
      FROM postgap
      GROUP BY gwas_source""").show(100, truncate=false)

    val aggregateByChr = ss.sql("""
      SELECT GRCh38_chrom, count(*) GRCh38_chrom_count
      FROM postgap
      GROUP BY GRCh38_chrom
      ORDER BY GRCh38_chrom_count DESC""").show(1000, truncate=false)

    // get filterout lines without the proper score levels at func genomics
    // also chromosome filter
    val filteredOTData = ss.sql(s"""
      SELECT *
      FROM postgap
      WHERE (vep_max_score >= 0.65 OR fg_score > 0 OR nearest = 1)
        AND gwas_source = 'GWAS Catalog'
        AND GRCh38_chrom IN ${PostgapData.chromosomesString}
        AND GRCh38_chrom = GRCh38_gene_chrom
        AND snp_gene_dist <= 1000000""")
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

  val parser:OptionParser[Config] = new OptionParser[Config](progName) {
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
      .action( (x, c) => c.copy(eco = x) )
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
