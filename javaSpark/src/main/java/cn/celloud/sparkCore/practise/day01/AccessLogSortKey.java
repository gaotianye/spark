package cn.celloud.sparkCore.practise.day01;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * N 次排序规则： 首先比较上行流量，如果上行流量形同，比较下行流量。如果下行流量相同，比较时间
 * 
 * @author Administrator
 *
 */
public class AccessLogSortKey implements Ordered<AccessLogSortKey>, Serializable {

	private static final long serialVersionUID = 1L;

	private long upTraffic;
	private long downTraffic;
	private long timestamp;

	/**
	 * 定义什么叫做大于
	 */
	public boolean $greater(AccessLogSortKey other) {
		if (upTraffic > other.upTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic > other.downTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp > other.timestamp) {
			return true;
		}
		return false;
	}

	/**
	 * 定义什么叫做大于等于
	 */
	public boolean $greater$eq(AccessLogSortKey other) {
		if ($greater(other)) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp) {
			return true;
		}
		return false;
	}

	/**
	 * 定义什么叫做小于
	 */
	public boolean $less(AccessLogSortKey other) {
		if (upTraffic < other.upTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic < other.downTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp < other.timestamp) {
			return true;
		}
		return false;
	}

	/**
	 * 定义什么叫做小于等于
	 */
	public boolean $less$eq(AccessLogSortKey other) {
		if($less(other)){
			return true;
		}else if(upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp){
			return true;
		}
		return false;
	}

	/**
	 * 定义什么比较大小
	 */
	public int compare(AccessLogSortKey other) {
		if (upTraffic - other.upTraffic != 0) {
			return (int) (upTraffic - other.upTraffic);
		} else if (downTraffic - other.downTraffic != 0) {
			return (int) (downTraffic - other.downTraffic);
		} else if (timestamp - other.timestamp != 0) {
			return (int) (timestamp - other.timestamp);
		}
		return 0;
	}

	/**
	 * 定义什么比较大小
	 */
	public int compareTo(AccessLogSortKey other) {
		if (upTraffic - other.upTraffic != 0) {
			return (int) (upTraffic - other.upTraffic);
		} else if (downTraffic - other.downTraffic != 0) {
			return (int) (downTraffic - other.downTraffic);
		} else if (timestamp - other.timestamp != 0) {
			return (int) (timestamp - other.timestamp);
		}
		return 0;
	}
	// ====================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (downTraffic ^ (downTraffic >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + (int) (upTraffic ^ (upTraffic >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AccessLogSortKey other = (AccessLogSortKey) obj;
		if (downTraffic != other.downTraffic)
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (upTraffic != other.upTraffic)
			return false;
		return true;
	}

	public AccessLogSortKey() {
		super();
	}

	public AccessLogSortKey(long upTraffic, long downTraffic, long timestamp) {
		super();
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
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

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
