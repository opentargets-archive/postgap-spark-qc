package postgapspark

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgapECO {
  type LookupTable = Map[String, (String, Double)]
  def generateLookupTable(ss: SparkSession, ecoFilename: String): LookupTable = {
    val ecoScores = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("mode", "DROPMALFORMED")
      .load(ecoFilename)

    ecoScores.collect.map(r => (r.getString(1), (r.getString(0), r.getDouble(2)))).toMap withDefaultValue(("",0))
  }

  private[postgapspark] def computeMaxVEP(vepTerms: String, lt: LookupTable): (String, Double) = {
    def splitVEPTerms(vepStr: String) = vepStr.trim.split(",").toSeq

    vepTerms match {
      case "N/A" => ("", 0)
      case vTerms =>
        splitVEPTerms(vTerms)
          .map(term => (term , lt(term)._2))
          .maxBy(_._2)
    }
  }
}
