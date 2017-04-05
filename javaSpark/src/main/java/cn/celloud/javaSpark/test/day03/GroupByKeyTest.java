package cn.celloud.javaSpark.test.day03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupByKeyTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75), 
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scoreList);
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = scoresRDD.groupByKey();
		/**
		 * 	class name : class1
			scores : [80, 90]
			class name : class2
			scores : [75, 65]
		 */
		groupByKeyRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Iterable<Integer>> values) throws Exception {
				System.out.println("class name : "+values._1);
				System.out.println("scores : "+values._2);
			}
		});
	}
}
