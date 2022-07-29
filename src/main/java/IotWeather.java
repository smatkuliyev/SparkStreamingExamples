import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class IotWeather {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Msı\\Downloads\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("SparkStreamingMessageListener").master("local").getOrCreate();

        StructType weatherType = new StructType()
                .add("quarter", "String")
                .add("heatType", "String")
                .add("heat", "integer")
                .add("windType", "String")
                .add("wind", "integer");

        Dataset<Row> rawData = sparkSession
                //.read()
                .readStream()
                .schema(weatherType)
                .option("sep", ",")
                .csv("C:\\Users\\Msı\\Desktop\\SparkStreamingListener\\spark-warehouse\\*");

        Dataset<Row> heatData = rawData.select("quarter", "heat", "wind").where("heat > 29 and wind > 35");
        //StreamingQuery start = heatData.writeStream().outputMode("complete").format("console").start(); //hata veriyor
        StreamingQuery start = heatData.writeStream().outputMode("append").format("console").start();
        start.awaitTermination();
    }
}
