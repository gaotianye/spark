package cn.celloud.javaSpark.sql.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 * HDFS测试
 * @author Administrator
 *
 */
public class LoadAndSaveTest2 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LoadAndSaveTest2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		/**
		 * 	|-- name: string (nullable = false)
			|-- favorite_color: string (nullable = true)
			|-- favorite_numbers: array (nullable = false)
			|    |-- element: integer (containsNull = false)
		 */
		//从本地读取数据
		DataFrame df = sqlContext.read().load("file:///users.parquet");
		/*df.printSchema();
		df.show();*/
		//存储到HDFS上xxx.parquet目录下
		df.select("name", "favorite_color").write().save("/gaotianye/spark/data/xxx.parquet");
	}
}
