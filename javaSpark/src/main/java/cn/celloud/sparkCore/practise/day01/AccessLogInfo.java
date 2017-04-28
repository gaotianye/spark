package cn.celloud.sparkCore.practise.day01;

import java.io.Serializable;
/**
 * 访问日志信息类（可序列化）
 */
public class AccessLogInfo implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private long timestamp;		// 时间戳
	private long upTraffic;		// 上行流量
	private long downTraffic;	// 下行流量
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public long getUpTraffic() {
		return upTraffic;
	}
	public void setUpTraffic(long upTraffic) {
		this.upTraffic = upTraffic;
	}
	public long getDownTraffic() {
		return downTraffic;
	}
	public void setDownTraffic(long downTraffic) {
		this.downTraffic = downTraffic;
	}
	public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
		super();
		this.timestamp = timestamp;
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
	}
	public AccessLogInfo() {
		super();
	}
	
}
