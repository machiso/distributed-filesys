package org.machi.dfs;

/**
 * NameNode核心启动类
 * @author machi
 *
 */
public class NameNode {

	/**
	 * NameNode是否在运行
	 */
	private volatile Boolean shouldRun;
	/**
	 * 负责管理元数据的核心组件：管理的是一些文件目录树，支持权限设置
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的Datanode的组件
	 */
	private DataNodeManager datanodeManager;
	/**
	 * NameNode对外提供rpc接口的server，可以响应请求
	 */
	private NameNodeRpcServer rpcServer;

	//负责对backupnode发送fsimage通信的server端
	private FSImageFileUploadServer fsImageFileUploadServer;
	
	public NameNode() {
		this.shouldRun = true;
	}
	
	/**
	 * 初始化NameNode
	 */
	private void initialize() {
		this.namesystem = new FSNamesystem(datanodeManager);
		this.datanodeManager = new DataNodeManager();
		this.rpcServer = new NameNodeRpcServer(this.namesystem, this.datanodeManager);
		this.fsImageFileUploadServer = new FSImageFileUploadServer();
	}
	
	/**
	 * 让NameNode运行起来
	 */
	public void start() throws Exception{
		rpcServer.start();
		rpcServer.blockUntilShutdown();
	}
		
	public static void main(String[] args) throws Exception {		
		NameNode namenode = new NameNode();
		namenode.initialize();
		namenode.start();
	}
	
}
