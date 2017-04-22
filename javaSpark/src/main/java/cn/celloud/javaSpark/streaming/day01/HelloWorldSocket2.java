package cn.celloud.javaSpark.streaming.day01;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * socket或者kakfa方式
 * @author Administrator
 */
public class HelloWorldSocket2 {
	public static void main(String[] args) {
		/**
		 * 2个线程，1个用来拉去数据，l个用来计算
		 * 如果只开启一个，那么只能看见提交job，不能看见计算
		 * 如果不设定setMaster，首先，必须要求集群节点上，有>1个cpu core，
		 * 其次，给Spark Streaming的每个executor分配的core，必须>1，
		 * 这样，才能保证分配到executor上运行的输入DStream，两条线程并行，一条运行Receiver，接收数据；
		 */
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HelloWorldSocket2");
		// 使用时JavaStremingContext 间隔1s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		/*
		 * 调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为Socket网络端口的
		 * 数据流，JavaReceiverInputStream，代表了一个输入的DStream---可以是从socket或者kafka获取数据
		 * socketTextStream()方法接收两个基本参数，第一个是监听哪个主机上的端口，第二个是监听哪个端口
		 */
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
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
