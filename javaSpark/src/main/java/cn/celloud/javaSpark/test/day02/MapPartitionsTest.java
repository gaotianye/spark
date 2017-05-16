package cn.celloud.javaSpark.test.day02;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * 函数会对每个分区依次调用分区函数处理，然后将处理的结果(若干个Iterator)生成新的RDDs。
 * mapPartitions与map类似，但是如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的多。
 * 比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大，
 * 如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
 * 
 * @author Administrator
 *
 */
public class MapPartitionsTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("mapPartitions").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
		// RDD有3个分区
		JavaRDD<Integer> javaRDD = sc.parallelize(data, 3);
		// 计算每个分区的合计
		JavaRDD<Integer> mapPartitionsRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Integer> call(Iterator<Integer> t) throws Exception {
				int sum = 0;
				while(t.hasNext()){
					Integer next = t.next();
					sum += next;
					System.out.println("now sum is :"+sum);
				}
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(sum);
				return list;
			}
		});
		System.out.println("mapPartitionsRDD~~~~~~~~~~~~~~~~~~~~~~" + mapPartitionsRDD.collect());
		sc.close();
	}
}
