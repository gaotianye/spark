package cn.celloud.javaSpark.test.day02;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * (1,2,3,4,5)==>(2,4,6,8,10)
 * @author Administrator
 *
 */
public class MapTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		//输入Integer  输出Integer
		JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		multipleNumberRDD.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Integer value) throws Exception {
				System.out.println(value);
			}
		});
		sc.close();
	}
}
