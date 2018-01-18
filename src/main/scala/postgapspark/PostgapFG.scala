package postgapspark

// functional genomics data filtering
object PostgapFG {
  /** return a pair with (string, double) as funcgen scores used and the total score computed */
  private[postgapspark] def computeFGScore(gtex: Double, fantom5: Double, dhs: Double, pchic: Double) = {
    val total: Seq[(String, Double)] = Seq(("GTEx", if (gtex > 0.999975) gtex * 13.0 else 0.0),
      ("FANTOM5", fantom5 * 3),
      ("DHS", dhs * 1.5),
      ("PCHiC", pchic * 1.5))

    val totalSum: Double = total.foldLeft[Double](0)((a, b) => a + b._2)
    val totalTags: String = total.filter(_._2 > 0)
      .foldLeft(List[String]())((b, pair) => b ::: List(pair._1))
      .mkString(",")

    (totalTags, 0.5 + (0.4 * totalSum / 19))
  }
}
