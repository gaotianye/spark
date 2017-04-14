package cn.celloud.javaSpark.test.day03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ReduceByKeyTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75), 
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scoreList);
		/**
		 * 先进行groupByKey，然后进行reduce操作。
		 * 在某些业务中（可以使用combiner的业务中），直接用reduceBykey比groupBykey的效率高。
		 */
		JavaPairRDD<String, Integer> totalScoresRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		/**
		 * 	class name : class1
			total scores : 170
			class name : class2
			total scores : 140
		 */
		totalScoresRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> values) throws Exception {
				System.out.println("class name : "+values._1);
				System.out.println("total scores : "+values._2);
			}
		});
	}
}
