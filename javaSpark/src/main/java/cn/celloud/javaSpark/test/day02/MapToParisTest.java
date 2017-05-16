package cn.celloud.javaSpark.test.day02;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * 从 单个 变成 一对
 * (1,2,3,4,5)===>{(1,"ok"),(2,"ok"),(3,"ok"),(4,"ok"),(5,"ok")}
 * @author Administrator
 *
 */
public class MapToParisTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("mapToPairs").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		JavaPairRDD<Integer, String> resultRDD = numberRDD.mapToPair(new PairFunction<Integer, Integer, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(Integer t) throws Exception {
				return new Tuple2<Integer, String>(t, "ok");
			}
		});
		resultRDD.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1+","+t._2);
			}
		});
		sc.close();
	}
}
