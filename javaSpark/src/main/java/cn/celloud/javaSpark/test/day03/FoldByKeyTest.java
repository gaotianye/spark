package cn.celloud.javaSpark.test.day03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * 	将rdd1中每个key对应的V进行累加，注意zeroValue=0,需要先初始化V,比如("A",0), ("A",2)，
 * 	先将zeroValue应用于每个V,得到：("A",0+0), ("A",2+0)，即：("A",0), ("A",2)，
 * 再将映射函数应用于初始化后的V，最后得到(A,0+2),即(A,2)
 *
 */
public class FoldByKeyTest {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("foldByKey").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75), 
				new Tuple2<String, Integer>("class3", 1), 
				new Tuple2<String, Integer>("class2", 34), 
				new Tuple2<String, Integer>("class4", 23), 
				new Tuple2<String, Integer>("class2", 67), 
				new Tuple2<String, Integer>("class3", 721), 
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class1", 10),
				new Tuple2<String, Integer>("class4", 20),
				new Tuple2<String, Integer>("class2", 30),
				new Tuple2<String, Integer>("class5", 50),
				new Tuple2<String, Integer>("class2", 65));
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scoreList);
		JavaPairRDD<String, Integer> foldByKey = scoresRDD.foldByKey(1, new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		/**
		 * 	class5	50
			class3	722
			class1	180
			class4	43
			class2	271
		 */
		foldByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"\t"+t._2);
			}
		});
	}
}
