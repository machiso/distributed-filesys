package org.machi.dfs;

import com.alibaba.fastjson.JSONArray;
import com.zhss.dfs.namenode.rpc.model.*;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import static org.machi.dfs.DataNodeConfig.*;

public class NameNodeRpcClient {

	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

	public NameNodeRpcClient() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}
	

	public boolean register() {
		// 通过RPC接口发送到NameNode他的注册接口上去
		RegisterRequest registerRequest = RegisterRequest.newBuilder()
				.setIp(DATANODE_IP)
				.setHostname(DATANODE_HOSTNAME)
				.build();
		RegisterResponse response = namenode.register(registerRequest);
		System.out.println("收到namenode返回的注册响应:"+response.getStatus());

		return response.getStatus() == 1 ? true : false;
	}


	public void startHeartbeat() {
		new HeartbeatThread().start();
	}

	//上报增量数据到namendoe
	public void informReplicaReceived(String filename) {
		InformReplicaReceivedRequest request = InformReplicaReceivedRequest.newBuilder()
				.setHostname(DATANODE_HOSTNAME)
				.setIp(DATANODE_IP)
				.setFilename(filename)
				.build();
		namenode.informReplicaReceived(request);
	}

	//全量上报
	public void reportCompleteStorageInfo(StorageInfo storageInfo) {
		ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder()
				.setIp(DATANODE_IP)
				.setHostname(NAMENODE_HOSTNAME)
				.setStoredDataSize(storageInfo.getStoredDataSize())
				.setFilenames(JSONArray.toJSONString(storageInfo.getFilenames()))
				.build();

		namenode.reportCompleteStorageInfo(request);
	}

	private class HeartbeatThread extends Thread{
		@Override
		public void run() {
			while (true){
				try {
					String ip = "127.0.0.1";
					String hostname = "dfs-data-01";

					HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
							.setIp(ip)
							.setHostname(hostname)
							.build();

					HeartbeatResponse heartbeatResponse = namenode.heartbeat(heartbeatRequest);
					System.out.println("收到namenode返回的心跳响应"+heartbeatResponse.getStatus());

					Thread.sleep(10 * 1000);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
