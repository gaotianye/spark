package cn.celloud.javaSpark.streaming.day02;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * kafka receiver 
 * TODO 未测试
 * @author Administrator
 */
public class HelloWorldKafkaReceiver {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HelloWorldKafkaReceiver");
		// 使用时JavaStremingContext 间隔5s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
		//put的第二个参数是 线程个数，调大它，并不影响spark并行度
		topicThreadMap.put("WordCount", 1);
		// 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc, 
				"zk1:2181,zk2:2181,zk3:2181", 
				"DefaultConsumerGroup", 
				topicThreadMap );
		/**
		 * 每次只计算这一批次的数据，上一批次的不会搭理。
		 */
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
				// 注意：Arrays.asList(line.split("\\s+"));正则表达式 多个空格
				return Arrays.asList(tuple._2.split("\\s+"));
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
