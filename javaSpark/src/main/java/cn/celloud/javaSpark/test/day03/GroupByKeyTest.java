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
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = scoresRDD.groupByKey();
		/**
			class name : class5
			scores : [50]
			class name : class3
			scores : [1, 721]
			class name : class1
			scores : [80, 90, 10]
			class name : class4
			scores : [23, 20]
			class name : class2
			scores : [75, 34, 67, 30, 65]
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
