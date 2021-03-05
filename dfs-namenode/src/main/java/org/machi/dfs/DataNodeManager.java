package org.machi.dfs;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 * @author machi
 *
 */
public class DataNodeManager {

	/**
	 * 内存中维护的datanode列表
	 */
	private Map<String,DataNodeInfo> datanodes = new ConcurrentHashMap<String,DataNodeInfo>();


	/**
	 * 开启心跳检测机制
	 */
	public DataNodeManager() {
		new DataNodeAliveMonitor().run();
	}

	/**
	 * datanode进行注册
	 * @param ip 
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname) {
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname);
		datanodes.put(ip+"-"+hostname,datanode);
		return true;
	}


	/**
	 * datanode心跳检测
	 * @param ip
	 * @param hostname
	 * @return
	 */
	public Boolean heartBeat(String ip, String hostname) {
		DataNodeInfo dataNodeInfo = new DataNodeInfo(ip,hostname);
		dataNodeInfo.setLastHeatBeatTime(System.currentTimeMillis());
		return true;
	}

	/**
	 * datanode心跳检测，检测datanode是否存活
	 */
	class DataNodeAliveMonitor extends Thread{
		@Override
		public void run() {
			final List<String> toRemoveList = new ArrayList<>();
			while (true){
				try {
					Iterator<DataNodeInfo> iteratorDataNode = datanodes.values().iterator();
					while (iteratorDataNode.hasNext()){
						DataNodeInfo nodeInfo = iteratorDataNode.next();
						//超过90s
						if (System.currentTimeMillis()-nodeInfo.getLastHeatBeatTime()>90*1000){
							toRemoveList.add(nodeInfo.getIp()+"-"+nodeInfo.getHostname());
						}
					}

					//移除
					Optional.ofNullable(toRemoveList).map(toRemove -> toRemoveList.remove(toRemove));

					Thread.sleep(30 * 1000);
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}
	
}
