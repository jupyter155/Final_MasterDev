
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

        // Define a Spark Session20
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Streaming")
//                .master("local")
//                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.scheduler.mode", "FAIR")
                .getOrCreate();

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.193.93:9092")
                .option("subscribe", "minhnx12_demo")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> DF =
                ds.selectExpr("CAST(value as string)")
                        .select(split(col("value"), ",").getItem(0).as("title"),
                                split(col("value"), ",").getItem(1).as("year"),
                                split(col("value"), ",").getItem(2).as("rating"),
                                split(col("value"), ",").getItem(3).as("runtime"),
                                split(col("value"), ",").getItem(4).as("kind"),
                                split(col("value"), ",").getItem(5).as("color_info"),
                                split(col("value"), ",").getItem(12).as("votes"),
                                split(col("value"), ",").getItem(13).as("country"),
                                split(col("value"), ",").getItem(14).as("day"),
                                split(col("value"), ",").getItem(15).as("month")
                        );

        DF.writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "/user/minhnx12/final/checkpoint") ///home/minh/Final_MasterDev/minhnx12/final/1
                .option("path", "/user/minhnx12/final/output") ///home/minh/Final_MasterDev/minhnx12/final/
                .start().awaitTermination();
    }
}
