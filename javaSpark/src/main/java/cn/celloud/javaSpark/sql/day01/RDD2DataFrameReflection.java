package cn.celloud.javaSpark.sql.day01;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 通过反射的方式 将RDD转成DataFrame，然后执行sql语句
 */
public class RDD2DataFrameReflection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		//读取数据源
		JavaRDD<String> lines = sc.textFile("students.txt");
		//map转换成Student对象
		JavaRDD<Student> students = lines.map(new Function<String, Student>() {
			private static final long serialVersionUID = 1L;
			public Student call(String line) throws Exception {
				Student stu = new Student();
				stu.setAge(Integer.parseInt(line.split(",")[2]));
				stu.setId(Integer.parseInt(line.split(",")[0]));
				stu.setName(line.split(",")[1]);
				return stu;
			}
		});
		// 使用反射方式，将RDD转换为DataFrame
		// 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		// 因为Student.class本身就是反射的一个应用
		// 然后底层还得通过对Student Class进行反射，来获取其中的field
		// 这里要求，JavaBean必须实现Serializable接口，是可序列化的
		DataFrame stuDf = sqlContext.createDataFrame(students, Student.class);
		// 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
		// hiveContext就不需要这么做
		stuDf.registerTempTable("my_students");
		// 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
		DataFrame teenageerDf = sqlContext.sql("select * from my_students where age <=18");
		// 将查询出来的DataFrame，再次转换为RDD
		JavaRDD<Row> javaRDD = teenageerDf.javaRDD();
		// 将RDD中的数据，进行映射，映射为Student
		JavaRDD<Student> map = javaRDD.map(new Function<Row, Student>() {
			private static final long serialVersionUID = 1L;

			public Student call(Row row) throws Exception {
				Student stu = new Student();
				stu.setId(row.getInt(1));
				stu.setName(row.getString(2));
				stu.setAge(row.getInt(0));
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
