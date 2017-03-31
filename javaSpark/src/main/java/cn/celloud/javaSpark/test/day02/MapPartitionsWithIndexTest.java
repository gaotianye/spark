package cn.celloud.javaSpark.test.day02;
import java.util.ArrayList;
/**
 * 分组进行map，且显示当前组id
 */
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsWithIndexTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> data = Arrays.asList("gaotianye","yuwei","wangyanzhong","zhangsanfeng",
				"zhangshuang","lihuihuan","qichengjian","yuweiwen","miaoqi","sunwendong");
		// RDD有3个分区
		JavaRDD<String> javaRDD = sc.parallelize(data, 3);
		// 计算每个分区的合计
		JavaRDD<String> indexRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				while(v2.hasNext()){
					String name = v2.next();
					list.add(v1+":"+name);
				}
				return list.iterator();
			}
		}, false);
		indexRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
}
