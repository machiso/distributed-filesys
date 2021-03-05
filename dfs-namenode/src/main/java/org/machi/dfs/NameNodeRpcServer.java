package org.machi.dfs;

/**
 * NameNode的rpc服务的接口
 * @author machi
 *
 */
public class NameNodeRpcServer {

	/**
	 * 负责管理元数据的核心组件
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private org.mac.fds.namenode.server.DataNodeManager datanodeManager;
	
	public NameNodeRpcServer(
			FSNamesystem namesystem, 
			org.mac.fds.namenode.server.DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否创建成功
	 * @throws Exception
	 */
	public Boolean mkdir(String path) throws Exception {
		return this.namesystem.mkdir(path);
	}
	
	/**
	 * DataNode进行注册
	 * @param ip
	 * @param hostname
	 * @return
	 * @throws Exception
	 */
	public Boolean register(String ip, String hostname) throws Exception {
		return datanodeManager.register(ip, hostname);
	}

	// 客户端发送心跳请求
	public Boolean heartBeat(String ip,String hostname) throws Exception{
		return datanodeManager.heartBeat(ip,hostname);
	}
	
	/**
	 * 启动这个rpc server
	 */
	public void start() {
		System.out.println("开始监听指定的rpc server的端口号，来接收请求");  
	}
	
}
