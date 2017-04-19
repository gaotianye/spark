package cn.celloud.javaSpark.sql.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 * 1、达成jar包。
 * 2、spark-submit --class cn.celloud.javaSpark.sql.day01.DataFrameTest1 
 *   /gaotianye/spark/demo/javaSpark-0.0.1-SNAPSHOT.jar
 */
public class DataFrameTest1 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("dataFrameTest1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().json("hdfs://iZ2ze484katmwnzbrkavhmZ:9000/student.json");
		df.show();
	}
}
