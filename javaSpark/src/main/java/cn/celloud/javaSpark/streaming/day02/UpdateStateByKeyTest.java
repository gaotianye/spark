package cn.celloud.javaSpark.streaming.day02;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

public class UpdateStateByKeyTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyTest");
		// 使用时JavaStremingContext 间隔5s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		/**
		 * 把每个key对应的state除了在内存中有，也要checkpoint一份,
		 * 以便于在内存数据丢失的时候，可以从checkpoint中恢复数据
		 * 
		 * 可以设定在当前目录（.），或者HDFS
		 */
		
		jsc.checkpoint("/gaotianye/data/checkpoint/");
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\\s+"));
			}
		});
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		/**
		 * 双十一大屏滚动信息就不能用reduceByKey，它仅仅能保证当前batch中的值，前面的无法计算。
		 * 使用 updateStateByKey,就可以实现直接通过Spark维护一份每个单词的全局的统计次数
		 * 这里的Optional，可以理解成Scala中的样例类，即Option。它代表了一个值的存在状态，可能存在，也可能不存在
		 */
		JavaPairDStream<String, Integer> result = pairs.updateStateByKey(
				new Function2<List<Integer>,Optional<Integer>, Optional<Integer>>() {
			private static final long serialVersionUID = 1L;
			/**
			 * 每次batch都会调用这个函数。
			 * 第一个参数：values，相当于是这个batch中，这个key的新的值，可能有多个吧。
			 * 例如：一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
			 * 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
			 */
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
				//这是一个全局的单词计数
				Integer newValue = 0;
				/*其次，判断，state是否存在。
				 * 如果不存在，说明是一个key第一次出现
				     如果存在，说明这个key之前已经统计过全局的次数了
				 */
				if(state.isPresent()){
					newValue = state.get();
				}
				//接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计次数
				for (Integer value : values) {
					newValue+=value;
				}
				//将这个key对应的值返回
				return Optional.of(newValue);
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
