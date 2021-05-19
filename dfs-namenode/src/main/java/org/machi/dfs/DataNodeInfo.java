package org.machi.dfs;

import lombok.Data;

/**
 * 用来描述datanode的信息
 * @author machi
 *
 */

@Data
public class DataNodeInfo implements Comparable<DataNodeInfo>{

	private final String ip;

	private final String hostname;

	//上次心跳发送的时间
	private long lastHeatBeatTime;

	//已经存储的数据大小
	private long storedDataSize;

	public DataNodeInfo(String ip,String hostname){
		this.ip = ip;
		this.hostname = hostname;
	}

	public long getLastHeatBeatTime() {
		return lastHeatBeatTime;
	}

	public void setLastHeatBeatTime(long lastHeatBeatTime) {
		this.lastHeatBeatTime = lastHeatBeatTime;
	}

	public long getStoredDataSize() {
		return storedDataSize;
	}

	public void setStoredDataSize(long storedDataSize) {
		this.storedDataSize = storedDataSize;
	}

	@Override
	public int compareTo(DataNodeInfo o) {
		if (this.getStoredDataSize() > o.getStoredDataSize()){
			return 1;
		}else if (this.getStoredDataSize() < o.getStoredDataSize()){
			return -1;
		}else {
			return 0;
		}

	}

	public void addStoredDataSize(long fileSize) {
		this.storedDataSize += fileSize;
	}
}
