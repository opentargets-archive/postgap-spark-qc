package postgapspark

import org.apache.spark.sql.types._

object PostgapData {
  // build the right data schema

  val Schema = StructType(
    StructField("ld_snp_rsID", StringType) ::
    StructField("chrom", StringType) ::
    StructField("pos", IntegerType) ::
    StructField("GRCh38_chrom", StringType) ::
    StructField("GRCh38_pos", IntegerType) ::
    StructField("afr_maf", StringType) :: // this should be double but do not drop it
    StructField("amr_maf", StringType) ::
    StructField("eas_maf", StringType) ::
    StructField("eur_maf", StringType) ::
    StructField("sas_maf", StringType) ::
    StructField("gene_symbol", StringType) ::
    StructField("gene_id", StringType) ::
    StructField("gene_chrom", StringType) ::
    StructField("gene_tss", IntegerType) ::
    StructField("GRCh38_gene_chrom", StringType) ::
    StructField("GRCh38_gene_pos", IntegerType) ::
    StructField("disease_name", StringType) ::
    StructField("disease_efo_id", StringType) ::
    StructField("score", DoubleType) ::
    StructField("rank", IntegerType) ::
    StructField("r2", DoubleType) ::
    StructField("cluster_id", LongType) ::
    StructField("gwas_source", StringType) ::
    StructField("gwas_snp", StringType) ::
    StructField("gwas_pvalue", DoubleType) ::
    StructField("gwas_pvalue_description", StringType) ::
    StructField("gwas_odds_ratio", StringType) ::
    StructField("gwas_beta", StringType) ::
    StructField("gwas_size", IntegerType) ::
    StructField("gwas_pmid", StringType) ::
    StructField("gwas_study", StringType) ::
    StructField("gwas_reported_trait", StringType) ::
    StructField("ls_snp_is_gwas_snp", IntegerType) ::
    StructField("vep_terms", StringType) ::
    StructField("vep_sum", DoubleType) ::
    StructField("vep_mean", DoubleType) ::
    StructField("GTEx", DoubleType) ::
    StructField("VEP", DoubleType) ::
    StructField("Fantom5", DoubleType) ::
    StructField("DHS", DoubleType) ::
    StructField("PCHiC", DoubleType) ::
    StructField("Nearest", IntegerType) ::
    StructField("Regulome", DoubleType) ::
    StructField("VEP_reg", DoubleType) :: Nil)
}
