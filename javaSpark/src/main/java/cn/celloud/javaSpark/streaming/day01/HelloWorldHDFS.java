package cn.celloud.javaSpark.streaming.day01;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * HDFS数据  只需要1个core就可以
 * @author Administrator
 */
public class HelloWorldHDFS {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HelloWorldHDFS");
		// 使用时JavaStremingContext 间隔5s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		// 首先，使用JavaStreamingContext的textFileStream()方法，针对HDFS目录创建输入数据流
		JavaDStream<String> lines = jsc.textFileStream("/gaotianye/data/");
		/**
		 * 每次只计算这一批次的数据，上一批次的不会搭理。
		 */
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			// 注意：Arrays.asList(line.split("\\s+"));正则表达式 多个空格
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split("\\s+"));
			}
		});
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairDStream<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		result.print();

		/*
		 * 首先对JavaSteamingContext进行一下后续处理
		 * 必须调用JavaStreamingContext的start()方法，整个Spark Streaming
		 * Application才会启动执行 否则是不会执行的
		 */
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
