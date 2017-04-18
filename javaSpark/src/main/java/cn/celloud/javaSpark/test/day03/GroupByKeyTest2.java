package cn.celloud.javaSpark.test.day03;
/**
 * 用groupByKey().map()来替代reduceByKey
 */
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupByKeyTest2 {
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
		/**
		 * 	class1:[80, 90]
			class2:[75, 65]
		 */
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = scoresRDD.groupByKey();
		JavaRDD<Tuple2<String, Integer>> map = groupByKeyRDD.map(new Function<Tuple2<String,Iterable<Integer>>, Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
				int sum = 0;
				Iterator<Integer> ite = v1._2.iterator();
				while(ite.hasNext()){
					sum+=ite.next();
				}
				return new Tuple2<String, Integer>(v1._1, sum);
			}
		});
		map.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("key:"+t._1+",value:"+t._2);
			}
		});
	}
}
