package cn.celloud.javaSpark.streaming.day02;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * kafka direct
 * 只需要1个线程即可
 * TODO 未测试
 * @author Administrator
 */
public class HelloWorldKafkaDirect {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HelloWorldKafkaDirect");
		// 使用时JavaStremingContext 间隔5s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		// 1、要创建一份kafka参数map  不是zookeeper信息
		Map<String, String> kafkaParams = new HashMap<String, String>();
		//第一次参数 broker list列表（固定的）                      第二个参数  kafka 9092端口
		kafkaParams.put("metadata.broker.list", "zoo1:9092,zoo2:9092,zoo3:9092");
		// 2、要创建一个set，里面放入，你要读取的topic，这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
		Set<String> topics = new HashSet<String>();
		topics.add("WordCount");
		
		// key和value默认String方式解析（一行行日志）
		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
				jsc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);
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
