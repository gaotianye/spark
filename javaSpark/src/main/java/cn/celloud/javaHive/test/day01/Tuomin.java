package cn.celloud.javaHive.test.day01;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Tuomin extends UDF{
	public Text evaluate(final Text s) {
		if (s == null) {
			return null;
		}
		String name = s.toString();
		name = name.substring(0,1)+"**";
		return new Text(name);
	}
}
