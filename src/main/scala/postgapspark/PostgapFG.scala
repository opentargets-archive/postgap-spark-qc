package postgapspark

// functional genomics data filtering
object PostgapFG {
  private[postgapspark] def computeFGScore(gtex: Double, fantom5: Double, dhs: Double, pchic: Double) = {
    val total: Seq[Double] = Seq(if (gtex > 0.999975) gtex * 13.0 else 0.0,
      fantom5 * 3,
      dhs * 1.5,
      pchic * 1.5)

    0.5 + (0.4 * total.sum / 19)
  }

  private[postgapspark] def computeNearest: Boolean = false
}
