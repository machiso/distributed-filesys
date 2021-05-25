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
	 * 集群中所有的datanode
	 */
	private Map<String, DataNodeInfo> datanodes = new ConcurrentHashMap<String, DataNodeInfo>();

	public DataNodeManager() {
		new DataNodeAliveMonitor().start();
	}

	/**
	 * datanode进行注册
	 * @param ip
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname,int nioPort) {
		if (datanodes.containsKey(ip + "-" + hostname)){
			return false;
		}
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname, nioPort);
		datanodes.put(ip + "-" + hostname, datanode);
		System.out.println("DataNode注册：ip=" + ip + ",hostname=" + hostname);
		return true;
	}

	/**
	 * datanode进行心跳
	 * @param ip
	 * @param hostname
	 * @return
	 */
	public Boolean heartbeat(String ip, String hostname) {
		DataNodeInfo datanode = datanodes.get(ip + "-" + hostname);
		if (datanode == null){
			//需要重新进行注册
			return false;
		}
		datanode.setLastHeatBeatTime(System.currentTimeMillis());
		System.out.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);
		return true;
	}

	//分配双副本对应的数据节点
	public List<DataNodeInfo> allocateDataNodes(long fileSize) {
		synchronized(this) {
			// 取出来所有的datanode，并且按照已经存储的数据大小来排序
			List<DataNodeInfo> datanodeList = new ArrayList<DataNodeInfo>();
			for(DataNodeInfo datanode : datanodes.values()) {
				datanodeList.add(datanode);
			}

			Collections.sort(datanodeList);

			// 选择存储数据最少的头两个datanode出来
			List<DataNodeInfo> selectedDatanodes = new ArrayList<DataNodeInfo>();
			if(datanodeList.size() >= 2) {
				selectedDatanodes.add(datanodeList.get(0));
				selectedDatanodes.add(datanodeList.get(1));

				//更新这两个datanode的已经存储的数据大小
				datanodeList.get(0).addStoredDataSize(fileSize);
				datanodeList.get(1).addStoredDataSize(fileSize);
			}
			return selectedDatanodes;
		}
	}

	public DataNodeInfo getDatanode(String ip, String hostname) {
		return datanodes.get(ip + "-" + hostname);
	}

	public void setStoredDataSize(String ip, String hostname, long storedDataSize) {
		DataNodeInfo datanode = datanodes.get(ip + "-" + hostname);
		datanode.setStoredDataSize(storedDataSize);
	}

	/**
	 * datanode是否存活的监控线程
	 * @author machi
	 *
	 */
	class DataNodeAliveMonitor extends Thread {

		@Override
		public void run() {
			try {
				while(true) {
					List<String> toRemoveDatanodes = new ArrayList<String>();

					Iterator<DataNodeInfo> datanodesIterator = datanodes.values().iterator();
					DataNodeInfo datanode = null;
					while(datanodesIterator.hasNext()) {
						datanode = datanodesIterator.next();
						if(System.currentTimeMillis() - datanode.getLastHeatBeatTime() > 90 * 1000) {
							toRemoveDatanodes.add(datanode.getIp() + "-" + datanode.getHostname());
						}
					}

					if(!toRemoveDatanodes.isEmpty()) {
						for(String toRemoveDatanode : toRemoveDatanodes) {
							datanodes.remove(toRemoveDatanode);
						}
					}

					Thread.sleep(30 * 1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
