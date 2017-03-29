package cn.celloud.javaSpark.test.day01;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 普通的wordcount，通过eclipse展示，输入文件是本地磁盘，输出在控制台上 输出时，按照count大小倒序，且word：count这么展示
 * 
 * @author Administrator
 *
 */
public class WordCount2 {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("wordcount-java-localdisk")
				.set("spark.testing.memory", "2147480000");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * gao tian ye hello world
		 */
		JavaRDD<String> linesRDD = sc.textFile("E:/test/a.txt");
		/**
		 * gao tian ye hello world
		 */
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		/**
		 * gao 1 tian 1 ye 1 hello 1 world 1
		 */
		JavaPairRDD<String, Integer> wordRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		/**
		 * gao 1 tian 1 ye 1 hello 1 world 1
		 */
		JavaPairRDD<String, Integer> resultRDD = wordRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		/**
		 * 将顺序颠倒过来。
		 * 1 gao 1 tian 1 ye 1 hello 1 world
		 */
		JavaPairRDD<Integer, String> reverseRDD = resultRDD
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				});
		// 倒序，然后再将颠倒的内容重新转回来
		resultRDD = reverseRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._2, t._1);
			}
		});
		// 控制台上展示
		resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> result) throws Exception {
				System.out.println("word is :" + result._1 + "  count is :" + result._2);
			}
		});
		sc.close();
	}
}
