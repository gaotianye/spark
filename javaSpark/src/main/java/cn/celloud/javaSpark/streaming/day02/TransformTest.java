package cn.celloud.javaSpark.streaming.day02;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * DStream.join()，只能join其他DStream。在DStream每个batch的RDD计算出来之后，会去跟其他DStream的RDD进行join。
 * 并没有提供将一个DStream中的每个batch，与一个特定的RDD进行join的操作。但是我们自己就可以使用transform操作来实现该功能。
 * 需求：实时黑名单过滤
 * 
 * @author Administrator
 *
 */
@SuppressWarnings("deprecation")
public class TransformTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TransformTest");
		// 使用时JavaStremingContext 间隔5s形成一个DStream
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		// 先做一份模拟的黑名单RDD,final类型
		List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
		blacklist.add(new Tuple2<String, Boolean>("tom", true));
		blacklist.add(new Tuple2<String, Boolean>("lilei", false));
		final JavaPairRDD<String, Boolean> blacklistRDD = jsc.sc().parallelizePairs(blacklist);
		JavaReceiverInputDStream<String> clickLogDstream = jsc.socketTextStream("localhost", 9999);
		/**
		 * 原始log日志：date username，为了方便join，转换成username,date username
		 */
		JavaPairDStream<String, String> userClickLogDstream = clickLogDstream
				.mapToPair(new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(String t) throws Exception {
						return new Tuple2<String, String>(t.split("\\s+")[1], t);
					}
				});
		/*
		 * 然后，就可以执行transform操作了.
		 * 将每个batch的RDD，与黑名单RDD进行join、filter、map等操作实时进行黑名单过滤
		 */
		JavaDStream<String> result = userClickLogDstream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			/*
			 * 这里为什么用左外连接？ 因为，并不是每个用户都存在于黑名单中的
			 * 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到, 就给丢弃掉了
			 * 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到也还是会被保存下来的.
			 */
			public JavaRDD<String> call(JavaPairRDD<String, String> user) throws Exception {
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = 
						user.leftOuterJoin(blacklistRDD);
				//执行filter，返回false的数据，即没有被过滤的数据
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {
					private static final long serialVersionUID = 1L;
					/*
					 *  这里的tuple，就是每个用户，对应的访问日志，和在黑名单中的状态
					 *  log:date username
					 *  (username,(log,true))
					 */
					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						if(tuple._2._2.isPresent() && tuple._2._2.get()){
							return false;
						}
						return true;
					}
				});
				//转换成我们想要的结果
				JavaRDD<String> logRDD = filteredRDD.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {
					private static final long serialVersionUID = 1L;

					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
						return tuple._2._1;
					}
				});
				return logRDD;
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
