package org.machi.dfs;

import lombok.Data;

/**
 * 用来描述datanode的信息
 * @author machi
 *
 */

@Data
public class DataNodeInfo {

	private final String ip;

	private final String hostname;

	//上次心跳发送的时间
	private long lastHeatBeatTime;

	public DataNodeInfo(String ip,String hostname){
		this.ip = ip;
		this.hostname = hostname;
	}

}
