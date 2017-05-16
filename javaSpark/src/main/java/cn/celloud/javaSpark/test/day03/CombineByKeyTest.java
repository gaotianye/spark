package cn.celloud.javaSpark.test.day03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CombineByKeyTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("combineByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75), 
				new Tuple2<String, Integer>("class3", 1), 
				new Tuple2<String, Integer>("class2", 34), 
				new Tuple2<String, Integer>("class4", 23), 
				new Tuple2<String, Integer>("class2", 67), 
				new Tuple2<String, Integer>("class3", 721), 
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class1", 10),
				new Tuple2<String, Integer>("class4", 20),
				new Tuple2<String, Integer>("class2", 30),
				new Tuple2<String, Integer>("class5", 50),
				new Tuple2<String, Integer>("class2", 65));
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scoreList);
		/**
		 *	1、在每个分区第一次出现键时，使用createCombiner()来创建那个键对应的累加器的初始值。
			2、执行完createCombiner()后，如果后边还有值，将会使用mergeValue()
				将该键的累加器对应的当前值与这个新值进行合并。
			3、由于每个分区都是独立处理的，因此对于同一个键可以有多个累加器。
				如果有两个或者更多的分区都有对应同一个键的累加器，
				就需要使用用户提供的mergeCombiners()将各个分区的结果进行合并。 
		 */
		Function<Integer, String> createCombiner = new Function<Integer, String>() {
			private static final long serialVersionUID = 1L;
			public String call(Integer v1) throws Exception {
				return v1 + ":";
			}
		};
		
		Function2<String, Integer, String> mergeValue = new Function2<String, Integer, String>() {
			private static final long serialVersionUID = 1L;
			public String call(String v1, Integer v2) throws Exception {
				return v1 + ":" + v2;   
			}
		};
		
		Function2<String, String, String> mergeCombiners = new Function2<String, String, String>() {
			private static final long serialVersionUID = 1L;
			public String call(String v1, String v2) throws Exception {
				return v1 + ":" + v2;
			}
		};
		
		JavaPairRDD<String, String> combinerByKeyRDD = scoresRDD.combineByKey(createCombiner, mergeValue, mergeCombiners);
		/**
		 * 	class5	50:
			class3	1::721
			class1	80::90:10
			class4	23::20
			class2	75::34:67:30:65
		 */
		combinerByKeyRDD.foreach(new VoidFunction<Tuple2<String,String>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t._1+"\t"+t._2);
			}
		});
		sc.close();
	}
}
