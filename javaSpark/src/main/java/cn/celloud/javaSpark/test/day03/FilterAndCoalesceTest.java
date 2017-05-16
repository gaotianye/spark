package cn.celloud.javaSpark.test.day03;
/**
 * filter之后，有的partition上的数据会被过滤掉
 * （有的partition数据多，而有的数据少，会造成数据倾斜），
 * 通过coalesce算子来让partition中的数据更加紧密。
 */
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class FilterAndCoalesceTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("coalesce").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		/**
		 * 默认是不用进行shuffle操作。
		 * partitions的个数不仅仅可以调小，还可以调大（当partitions个数少，而每个partition数据很多时）。
		 * 此时需要将shuffle设定为true，如果不设定成true，将不会起到作用。
		 * 当partition个数发生剧烈变化时，也需要将shuffle值设定成true。
		 */
		JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {

			private static final long serialVersionUID = 1L;

			public Boolean call(Integer v1) throws Exception {
				if(v1%2==0){
					return true;
				}
				return false;
			}
		}).coalesce(2);
		evenNumberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			public void call(Integer value) throws Exception {
				System.out.println(value);
			}
		});
		sc.close();
	}
}
