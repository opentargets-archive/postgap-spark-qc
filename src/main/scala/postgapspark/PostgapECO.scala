package postgapspark

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgapECO {
  type LookupTable = Map[String, (String, Double)]
  def generateLookupTable(ss: SparkSession): LookupTable = {
    val ecoScores = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(filePath)

    ecoScores.collect.map(r => (r.getString(1), (r.getString(0), r.getDouble(2)))).toMap withDefaultValue(("",0))
  }

  private[postgapspark] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("postgapspark/eco_scores.tsv")
    if (resource == null) sys.error("We need the eco_scores.tsv file to compute vep term scores")
    new File(resource.toURI).getPath
  }

  private[postgapspark] def computeMaxVEP(vepTerms: String, lt: LookupTable): Double = {
    def splitVEPTerms(vepStr: String) = vepStr.trim.split(",").toSeq

    vepTerms match {
      case "N/A" => 0
      case vTerms =>
        splitVEPTerms(vTerms)
          .map(lt(_)._2)
          .max
    }
  }
}
