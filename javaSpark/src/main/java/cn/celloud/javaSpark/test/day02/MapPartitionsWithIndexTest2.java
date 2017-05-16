package cn.celloud.javaSpark.test.day02;
import java.util.ArrayList;
/**
 * mapPartitions和mapPartitionsWithIndex两个案例一起测试
 * mapPartitionsWithIndex的第二个参数是什么意思？？？？？？
 */
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsWithIndexTest2 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1,2,3,4,5,6,7);
		// RDD有3个分区
		JavaRDD<Integer> javaRDD = sc.parallelize(data, 3);
		// 计算每个分区的合计
		JavaRDD<String> indexRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
				int sum = 0;
				ArrayList<String> list = new ArrayList<String>();
				while(v2.hasNext()){
					Integer next = v2.next();
					list.add(v1+","+next);
					sum +=next;
					System.out.println("now sum is :"+sum);
				}
				return list.iterator();
			}
		}, true);
		indexRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			public void call(String s) throws Exception {
				System.out.println(s);
			}
		});
		sc.close();
	}
}
