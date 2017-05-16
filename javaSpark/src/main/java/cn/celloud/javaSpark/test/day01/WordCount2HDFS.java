package cn.celloud.javaSpark.test.day01;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 普通的wordcount，通过linux系统展示，输入文件是HDFS，输出在HDFS 输出时，按照count大小倒序，且word：count这么展示
 * 
 * @author Administrator
 *
 */
public class WordCount2HDFS {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: WordCount2HDFS <input> <output>");
			// 非正常退出 System.exit(0)代表正常退出
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("wordcount-java-hdfs")
				.set("spark.testing.memory", "2147480000");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * gao tian ye hello world
		 */
		JavaRDD<String> linesRDD = sc.textFile(args[0]);
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
		 * 1 gao 1 tian 1 ye 1 hello 1 world
		 */
		JavaPairRDD<Integer, String> reverseRDD = resultRDD
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				});
		// 倒序，然后再转回来
		resultRDD = reverseRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._2, t._1);
			}
		});
		// 保存到HDFS中
		resultRDD.saveAsTextFile(args[1]);
		sc.close();
	}
}
