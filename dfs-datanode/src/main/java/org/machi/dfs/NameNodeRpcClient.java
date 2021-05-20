package org.machi.dfs;

import com.zhss.dfs.namenode.rpc.model.InformReplicaReceivedRequest;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 负责跟一组NameNode进行通信的OfferServie组件
 * @author machi
 *
 */
public class NameNodeRpcClient {

	/**
	 * 负责跟NameNode主节点通信的ServiceActor组件
	 */
	private NameNodeServiceActor activeServiceActor;
	
	/**
	 * 构造函数
	 */
	public NameNodeRpcClient() {
		this.activeServiceActor = new NameNodeServiceActor();
	}
	
	/**
	 * 启动OfferService组件
	 */
	public void start() {
		// 直接使用两个ServiceActor组件分别向主备两个NameNode节点进行注册
		register();
		// 开始发送心跳
		startHeartbeat();
	}
	
	/**
	 * 向主备两个NameNode节点进行注册
	 */
	private void register() {
		try {
			this.activeServiceActor.register();
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}

	/**
	 * 开始发送心跳给NameNode
	 */
	private void startHeartbeat() {
		this.activeServiceActor.startHeartbeat();
	}

	//上报增量数据到namendoe
	public void informReplicaReceived(String relativeFilename) {
		activeServiceActor.informReplicaReceived(relativeFilename);
	}
}
