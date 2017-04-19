package cn.celloud.javaSpark.sql.day01;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 通过动态的的方式 将RDD转成DataFrame，然后执行sql语句
 */
public class RDD2DataFrameDynamic {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameDynamic");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		//读取数据源
		JavaRDD<String> lines = sc.textFile("students.txt");
		//map转换成Student对象
		JavaRDD<Row> students = lines.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;
			public Row call(String line) throws Exception {
				String[] split = line.split(",");
				//row对象不是通过new出来的，而是通过工厂方法
				Row row = RowFactory.create(Integer.parseInt(split[0]),
						split[1],Integer.parseInt(split[2]));
				return row;
			}
		});
		// 第二步，动态构造元数据
		// 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
		// 或者是配置文件中，加载出来的，是不固定的
		// 所以特别适合用这种编程的方式，来构造元数据
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);
		// 第三步，使用动态构造的元数据，将RDD转换为DataFrame
		DataFrame sdf = sqlContext.createDataFrame(students, structType);
		sdf.registerTempTable("students");
		DataFrame sqldf = sqlContext.sql("select * from students where age >=15");
		/*List<Row> lists = sqldf.javaRDD().collect();
		for (Row row : lists) {
			System.out.println(row);
		}*/
		// 将查询出来的DataFrame，再次转换为RDD
		JavaRDD<Row> javaRDD = sqldf.javaRDD();
		// 将RDD中的数据，进行映射，映射为Student
		JavaRDD<Student> map = javaRDD.map(new Function<Row, Student>() {
			private static final long serialVersionUID = 1L;

			public Student call(Row row) throws Exception {
				Student stu = new Student();
				int id = row.getAs("id");
				String name = row.getAs("name");
				int age = row.getAs("age");
				stu.setId(id);
				stu.setName(name);
				stu.setAge(age);
				return stu;
			}
		});
		// 将数据collect回来，打印出来
		List<Student> collect = map.collect();
		for (Student student : collect) {
			System.out.println(student.getId()+","+student.getName()+","+student.getAge());
		}
	}
}
