package cn.celloud.javaSpark.sql.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 *	补充：默认情况下，如果不指定数据源类型，那么就是parquet。
	也就是说：这么写是错误的：
	DataFrame df = sqlContext.read().load("students.json");
	
	另外，好像format只支持  json、jdbc、parquet
 */
public class LoadAndSaveTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadAndSaveTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame df = sqlContext.read().format("json").load("/people.json");
		df.printSchema();
		df.show();
		df.select("name").write().format("json").save("/test");
	}
}
