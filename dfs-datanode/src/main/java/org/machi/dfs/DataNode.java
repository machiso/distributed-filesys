package org.machi.dfs;

public class DataNode {

	private volatile Boolean shouldRun;

	private NameNodeRpcClient nameNodeRpcClient;

	private StorageManager storageManager;

	private HeartBeatManager heartBeatManager;

	public DataNode() {
		this.shouldRun = true;
		this.nameNodeRpcClient = new NameNodeRpcClient();
		//注册成功之后再全量上报
		if (!nameNodeRpcClient.register()){
			System.out.println("向NameNode注册失败，直接退出......");
			System.exit(1);
		}

		//storageManager组件
		this.storageManager = new StorageManager();
		StorageInfo storageInfo = storageManager.getStorageInfo();
		if (storageInfo != null){
			//全量上报
			nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
		}

		//心跳组件
		heartBeatManager = new HeartBeatManager(nameNodeRpcClient,storageManager);
		heartBeatManager.start();

		DataNodeNIOServer nioServer = new DataNodeNIOServer(nameNodeRpcClient);
		nioServer.start();
	}

	/**
	 * 运行DataNode
	 */
	private void start() {
		try {
			while(shouldRun) {
				Thread.sleep(1000);  
			}   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		DataNode datanode = new DataNode();
		datanode.start();
	}
	
}
