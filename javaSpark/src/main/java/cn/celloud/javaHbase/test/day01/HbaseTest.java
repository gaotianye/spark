package cn.celloud.javaHbase.test.day01;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * TODO 由于Hadoop jar包问题，报错。。。。。。。。。
 * TODO 是否是阿里云的zookeeper报错？？
 * TODO 如何和hive结合呢？
 * 
 * 本机号码    呼叫类型（主叫-1/被叫-0）    通话时长    时间    对方号码
 * 
 * 练习1：插入 10个手机号（10个用户），每个用户100条通话记录
 * 练习2：查询指定月份的通话详单
 * 练习3：查询某个手机号所有主叫类型（type=0）的详单--使用过滤器
 * 
 * @author Administrator
 *
 */
/*public class HbaseTest {
	HBaseAdmin hadmin = null;
	String tableName = "test_tbl1";
	String familyName = "cf1";
	HTable htable = null;
	
	@Before
	public void begin() throws Exception{
		Configuration conf = new Configuration();
		//不要将配置文件放在src下面
		conf.set("hbase.zookeeper.quorum", "47.93.51.90,47.93.42.221,47.93.40.163");
		hadmin = new HBaseAdmin(conf);
		htable = new HTable(conf, tableName);
	}
	
	@After
	public void end() throws Exception{
		if(hadmin!=null){
			hadmin.close();
		}
		if(htable!=null){
			htable.close();
		}
	}
	*//**
	 * 创建表
	 * @throws Exception
	 *//*
	@Test
	public void createTable() throws Exception{
		//判断表是否存在，如果存在，先disable，然后drop
		if(hadmin.tableExists(tableName)){
			hadmin.disableTable(tableName);
			hadmin.deleteTable(tableName);
		}
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		//建议只有1-3个列族
		HColumnDescriptor family = new HColumnDescriptor(familyName);
		//设定读缓存，提高我们的效率（默认也是启动的）
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		tableDesc.addFamily(family);
		
		hadmin.createTable(tableDesc);
		System.out.println("create table ok .............");
	}
	
	*//**
	 * 往表中插入数据
	 * @throws Exception
	 *//*
	@Test
	public void insert() throws Exception{
		//时间戳
		String rowKey = "15156506755_201704121111";
		Put put = new Put(rowKey.getBytes());
		put.addColumn(familyName.getBytes(), "type".getBytes(), "1".getBytes());
		put.addColumn(familyName.getBytes(), "time".getBytes(), "2017".getBytes());
		//目标手机号
		put.addColumn(familyName.getBytes(), "pnum".getBytes(), "13552879121".getBytes());
		htable.put(put);
	}
	
	*//**
	 * 查看表信息
	 * @throws Exception
	 *//*
	@Test
	public void get() throws Exception{
		//时间戳
		String rowKey = "15156506755_201704121111";
		Get get = new Get(rowKey.getBytes());
		get.addColumn(familyName.getBytes(), "type".getBytes());
		get.addColumn(familyName.getBytes(), "time".getBytes());
		Result result = htable.get(get);
		
		*//**
		 * 问：为什么前文get时已经指定了列，下面拿的时候还要指定一次呢？
		 * 答：hbase是列式存储，列可以随意添加，一行数据的列有很多，查询时只需要查询指定的列即可。
		 *    result结果值中，同样只需要读想要读的内容。当读的内容 在查询时没有执行，则报错。
		 *//*
		Cell cell = result.getColumnLatestCell(familyName.getBytes(), "type".getBytes());
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}
	
	
	Random r = new Random();
	*//**
	 * 随机生成手机号码
	 * @param prefix
	 * @return
	 *//*
	public String getPhoneNum(String prefix){
		return prefix+String.format("%08d", r.nextInt(99999999));
	}
	
	*//**
	 * 随机生成时间
	 * @param year
	 * @return
	 *//*
	public String getDate(String year){
		return year + String.format("%02d%02d%02d%02d%02d", new Object[]{
			r.nextInt(12)+1,r.nextInt(29)+1,r.nextInt(23),r.nextInt(60),r.nextInt(60)	
		});
	}
	
	*//**
	 * 练习1：插入 10个手机号（10个用户），每个用户100条通话记录
	 * 满足查询  时间降序排序
	 * @throws Exception 
	 *//*
	@Test
	public void insertDB() throws Exception{
		List<Put> puts = new ArrayList<Put>();
		for(int i = 0;i<10;i++){
			String rowKey;
			//生成手机号
			String phoneNum = getPhoneNum("186");
			for(int j=0;j<100;j++){
				//生成通话时间
				String phoneDate = getDate("2016");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				long dataLong = sdf.parse(phoneDate).getTime();
				rowKey = phoneNum + (Long.MAX_VALUE-dataLong);
				
				Put put = new Put(rowKey.getBytes());
				put.add(familyName.getBytes(), "type".getBytes(), (r.nextInt(2)+"").getBytes());
				put.add(familyName.getBytes(), "time".getBytes(), (phoneDate).getBytes());
				put.add(familyName.getBytes(), "pnum".getBytes(), (getPhoneNum("170")).getBytes());
				puts.add(put);
			}
		}
		htable.put(puts);
	}
	
	*//**
	 * 练习2：查询指定月份的通话详单
	 * 展示信息如下：3月
	 * 31号 
	 * 30号
	 * .....
	 * @throws Exception
	 *//*
	@Test
	public void scanDB() throws Exception{
		Scan scan = new Scan();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		String startRowkey = "15156506755"+(Long.MAX_VALUE- sdf.parse("20160301000000").getTime());
		scan.setStartRow(startRowkey.getBytes());
		
		String endRowkey = "15156506755"+(Long.MAX_VALUE- sdf.parse("20160201000000").getTime());
		scan.setStopRow(endRowkey.getBytes());
		
		ResultScanner res = htable.getScanner(scan);
		for (Result result : res) {
			System.out.println(new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "type".getBytes())))+
					new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "time".getBytes())))+
							new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "pnum".getBytes()))));
		}
	}
	
	*//**
	 * 查询某个手机号所有  主叫/被叫类型详单（例如，主叫type=0）
	 * 使用过滤器来解决，但是效率不是很高
	 * 通常将主要查询的业务，放在rowkey中。例如现在rowkey：手机号_时间（上边代码解决了如何让其最后倒序显示时间）
	 * rowkey长度：64kb是最大值
	 * @throws Exception
	 *//*
	@Test
	public void scanDB2() throws Exception{
		//所有过滤条件都要用上
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		//前置过滤器
		PrefixFilter prefixFilter = new PrefixFilter("15156506755".getBytes());
		
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(familyName.getBytes(),"type".getBytes(),
				CompareOp.EQUAL,Bytes.toBytes("0"));
		list.addFilter(prefixFilter);
		list.addFilter(filter1);
		
		Scan scan = new Scan();
		scan.setFilter(list);
		
		ResultScanner res = htable.getScanner(scan);
		for (Result result : res) {
			byte[] row = result.getColumnLatestCell(familyName.getBytes(), "type".getBytes()).getRow();
			System.out.println(new String(row)+
					new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "type".getBytes())))+
					new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "time".getBytes())))+
							new String(CellUtil.cloneValue(result.getColumnLatestCell(familyName.getBytes(), "pnum".getBytes()))));
		}
	}
	
	@Test
	public void test1() throws Exception{
		String d = "20170301000000";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Date parse = sdf.parse(d);
		System.out.println(parse);
	}
}*/
