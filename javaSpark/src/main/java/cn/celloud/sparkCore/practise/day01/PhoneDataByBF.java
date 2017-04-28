package cn.celloud.sparkCore.practise.day01;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * 先将同一个用户的上行和下行相加。然后进行二次排序（倒序）
 * N次排序规则：首先比较上行流量，如果上行流量形同，比较下行流量。如果下行流量相同，比较时间
 * 
 * 思想：将deviceID（用户标识）排除在外，其他的组成对象。
 */
public class PhoneDataByBF {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("PhoneDataByBF");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//读取log日志
		JavaRDD<String> accessLogRDD = sc.textFile("hdfs://xxx:9000/prictise/access.log");
		//将log日志RDD转换成key-value格式，key：用户标识   value：LogInfo对象
		JavaPairRDD<String,AccessLogInfo> accessLogPairRDD = mapAccessLogRDD2Pair(accessLogRDD);
		//安装用户标识，将用户所有的上行和下行相加 key:用户标识    value：loginfo对象（time：最早的   上行和下行累加和）
		JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD = aggregateByDeviceId(accessLogPairRDD);
		//将新的RDD的key映射为二次排序key.这样，sortBykey时，就会更加自定义排序的key 来排序。
		JavaPairRDD<AccessLogSortKey, String> accessLogSortRDD = mapRDDKey2SortKey(aggrAccessLogPairRDD);
		//执行二次排序
		JavaPairRDD<AccessLogSortKey, String> sortByKey = accessLogSortRDD.sortByKey(false);
		//获取前10位
		List<Tuple2<AccessLogSortKey, String>> take = sortByKey.take(10);
		
		for (Tuple2<AccessLogSortKey, String> tuple2 : take) {
			System.out.println(tuple2._2+":"+tuple2._1);
		}
		sc.close();
	}
	
	/**
	 * 将原始accessLogRDD转换成pairRDD 
	 * key：用户标识     value：LogInfo对象
	 * @param accessLogRDD
	 * @return
	 */
	private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(JavaRDD<String> accessLogRDD) {
		JavaPairRDD<String, AccessLogInfo> mapToPair = accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, AccessLogInfo> call(String log) throws Exception {
				String[] split = log.split("\t");
				AccessLogInfo info = new AccessLogInfo();
				info.setTimestamp(Long.valueOf(split[0]));
				info.setUpTraffic(Long.valueOf(split[2]));
				info.setDownTraffic(Long.valueOf(split[3]));
				String deviceID = split[1];
				return new Tuple2<String, AccessLogInfo>(deviceID, info);
			}
		});
		return mapToPair;
	}
	
	/**
	 * 将同一个用户的上行和下行累加。然后只展示这个用户第一次出现的时刻。
	 * @param accessLogPairRDD
	 * @return
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceId(JavaPairRDD<String, AccessLogInfo> accessLogPairRDD){
		JavaPairRDD<String, AccessLogInfo> reduceByKeyRDD = accessLogPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
			private static final long serialVersionUID = 1L;

			public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2) throws Exception {
				long timestamp = v1.getTimestamp() < v2.getTimestamp()?v1.getTimestamp():v2.getTimestamp();
				long upTraffic = v1.getUpTraffic()+v2.getUpTraffic();
				long downTraffic = v1.getDownTraffic()+v2.getDownTraffic();
				
				AccessLogInfo info = new AccessLogInfo();
				info.setTimestamp(timestamp);
				info.setDownTraffic(downTraffic);
				info.setUpTraffic(upTraffic);
				
				return info;
			}
		});
		return reduceByKeyRDD;
	}
	
	/**
	 * 原RDD的key：deviceID   value：上行和下行累加和，且用户第一次出现的时间
	 * 将RDD的key映射为二次排序key.这样，sortBykey时，就会更加自定义排序的key 来排序。
	 * 新RDD的key：  value：
	 * @param aggrAccessLogPairRDD
	 * @return
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<AccessLogSortKey, String> mapRDDKey2SortKey(JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD){
		JavaPairRDD<AccessLogSortKey, String> mapToPair = aggrAccessLogPairRDD.mapToPair(new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> tuple) throws Exception {
				String deviceId = tuple._1;
				AccessLogInfo logInfo = tuple._2;
				// 将日志信息封装为二次排序key
				AccessLogSortKey sortKey = new AccessLogSortKey(logInfo.getUpTraffic(), 
						logInfo.getDownTraffic(), logInfo.getTimestamp());
				return new Tuple2<AccessLogSortKey, String>(sortKey, deviceId);
			}
		});
		return mapToPair;
	}
}
