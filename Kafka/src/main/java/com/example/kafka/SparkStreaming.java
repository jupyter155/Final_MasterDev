package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class SparkStreaming {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.OFF);

        // Define a Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Streaming")
                .master("local")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.17.80.23:9092")
                .option("subscribe", "minhnx12")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> DF =
                ds.selectExpr("CAST(value as string)")
                        .select(split(col("value"),",").getItem(0).as("title"),
//                                split(col("value"),",").getItem(1).as("year"),
//                                split(col("value"),",").getItem(2).as("rating"),
//                                split(col("value"),",").getItem(3).as("runtime"),
//                                split(col("value"),",").getItem(4).as("kind"),
//                                split(col("value"),",").getItem(5).as("color_info"),
                                split(col("value"),",").getItem(12).as("votes")
                                );
//        DF.show();
        DF.writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "/home/minh/Final_MasterDev/minhnx12/final/1") ///home/minh/Final_MasterDev/minhnx12/final/1
                .option("path", "/home/minh/Final_MasterDev/minhnx12/final/2") ///home/minh/Final_MasterDev/minhnx12/final/2
                .start().awaitTermination();
    }
}


