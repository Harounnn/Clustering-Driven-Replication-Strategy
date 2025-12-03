from pyspark.sql import SparkSession, functions as F, types as T
import argparse
from datetime import datetime

def iso_to_ts(iso):
    try:
        return datetime.fromisoformat(iso.replace("Z","+00:00")).timestamp()
    except:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--access_log", required=True)
    parser.add_argument("--out", default="features.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("compute_hdfs_features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    manifest_df = spark.read.option("header","true").csv(args.manifest)

    manifest_df = manifest_df.withColumn("creation_ts_epoch",
                                         F.unix_timestamp("creation_ts","yyyy-MM-dd'T'HH:mm:ssX").cast("double"))

    schema = T.StructType([
        T.StructField("ts_iso", T.StringType(), True),
        T.StructField("path", T.StringType(), True),
        T.StructField("op", T.StringType(), True),
        T.StructField("client_node", T.StringType(), True),
        T.StructField("pid", T.StringType(), True)
    ])
    access_df = spark.read.option("header","false").schema(schema).csv(args.access_log)

    access_df = access_df.withColumn("ts_epoch", F.unix_timestamp("ts_iso","yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("double"))

    access_df = access_df.withColumn("ts_epoch", F.when(F.col("ts_epoch").isNull(),
                                                        F.unix_timestamp("ts_iso","yyyy-MM-dd'T'HH:mm:ssX"))\
                                                .otherwise(F.col("ts_epoch")))


    freq_df = access_df.groupBy("path").agg(
        F.count(F.lit(1)).alias("access_freq"),
        F.sum(F.when(F.col("op")=="WRITE",1).otherwise(0)).alias("writes"),
        F.sum(F.when(F.col("op")=="READ",1).otherwise(0)).alias("reads")
    )


    access_with_primary = access_df.join(manifest_df.select("path","primary_node"), on="path", how="left")
    locality_df = access_with_primary.withColumn("is_local",
                                                F.when(F.col("client_node")==F.col("primary_node"),1).otherwise(0)) \
                                     .groupBy("path") \
                                     .agg(F.sum("is_local").alias("local_accesses"),
                                          F.count(F.lit(1)).alias("total_accesses"))

    conc_df = access_df.withColumn("sec", F.floor("ts_epoch")) \
                       .groupBy("path","sec").agg(F.count("*").alias("concurrent_in_sec")) \
                       .groupBy("path").agg(F.max("concurrent_in_sec").alias("max_concurrency"))

    max_ts_row = access_df.agg(F.max("ts_epoch").alias("max_ts")).collect()[0]
    observation_end = max_ts_row["max_ts"]
    if observation_end is None:
        observation_end = F.current_timestamp()

    age_df = manifest_df.select("path","creation_ts_epoch") \
                        .withColumn("age_seconds", F.lit(observation_end) - F.col("creation_ts_epoch"))

    joined = manifest_df.select("path").join(freq_df, on="path", how="left") \
                         .join(locality_df, on="path", how="left") \
                         .join(conc_df, on="path", how="left") \
                         .join(age_df.select("path","age_seconds"), on="path", how="left") \
                         .na.fill({'access_freq':0, 'writes':0, 'reads':0, 'local_accesses':0, 'total_accesses':0, 'max_concurrency':0, 'age_seconds':0})

    mean_writes = joined.agg(F.mean("writes").alias("mu_writes")).collect()[0]["mu_writes"]
    if mean_writes == 0:
        mean_writes = 1.0 
    joined = joined.withColumn("write_ratio", F.col("writes")/F.lit(mean_writes))

    joined = joined.withColumn("locality", F.when(F.col("total_accesses")>0, F.col("local_accesses")/F.col("total_accesses")).otherwise(F.lit(1.0)))

    res = joined.select("path",
                        "access_freq",
                        "age_seconds",
                        "write_ratio",
                        "locality",
                        F.col("max_concurrency").alias("concurrency"))

    stats = res.agg(
        F.min("access_freq").alias("min_af"), F.max("access_freq").alias("max_af"),
        F.min("age_seconds").alias("min_age"), F.max("age_seconds").alias("max_age"),
        F.min("write_ratio").alias("min_wr"), F.max("write_ratio").alias("max_wr"),
        F.min("locality").alias("min_loc"), F.max("locality").alias("max_loc"),
        F.min("concurrency").alias("min_con"), F.max("concurrency").alias("max_con"),
    ).collect()[0]

    def minmax(col, minv, maxv):
        return F.when(F.lit(maxv)==F.lit(minv), F.lit(0.0)).otherwise((F.col(col)-F.lit(minv)) / (F.lit(maxv)-F.lit(minv)))

    res = res.withColumn("access_freq_norm", minmax("access_freq", stats["min_af"], stats["max_af"])) \
             .withColumn("age_norm", minmax("age_seconds", stats["min_age"], stats["max_age"])) \
             .withColumn("write_ratio_norm", minmax("write_ratio", stats["min_wr"], stats["max_wr"])) \
             .withColumn("locality_norm", minmax("locality", stats["min_loc"], stats["max_loc"])) \
             .withColumn("concurrency_norm", minmax("concurrency", stats["min_con"] , stats["max_con"]))

    res.coalesce(1).write.option("header","true").csv(args.out)

    spark.stop()
    print("Wrote features to", args.out)
