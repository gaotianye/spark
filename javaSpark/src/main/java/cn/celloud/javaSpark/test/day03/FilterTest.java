package cn.celloud.javaSpark.test.day03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FilterTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(Integer v1) throws Exception {
				if(v1%2==0){
					return true;
				}
				return false;
			}
		});
		evenNumberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			public void call(Integer value) throws Exception {
				System.out.println(value);
			}
		});
		sc.close();
	}
}
