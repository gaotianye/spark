package cn.celloud.javaSpark.sql.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 * 本地测试有问题
 * @author Administrator
 *
 */
public class LoadAndSaveTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadAndSaveTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		/**
		 * 	|-- name: string (nullable = false)
			|-- favorite_color: string (nullable = true)
			|-- favorite_numbers: array (nullable = false)
			|    |-- element: integer (containsNull = false)
		 */
		DataFrame df = sqlContext.read().load("users.parquet");
		/*df.printSchema();
		df.show();*/
		df.select("name", "favorite_color").write().save("C:\\Users\\Administrator\\Desktop\\test.parquet");
	}
}
