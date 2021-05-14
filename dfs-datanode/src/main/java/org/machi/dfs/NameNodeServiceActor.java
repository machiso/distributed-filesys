package org.machi.dfs;

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import com.zhss.dfs.namenode.rpc.model.HeartbeatRequest;
import com.zhss.dfs.namenode.rpc.model.HeartbeatResponse;
import com.zhss.dfs.namenode.rpc.model.RegisterRequest;
import com.zhss.dfs.namenode.rpc.model.RegisterResponse;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;


/**
 * 负责跟一组NameNode中的某一个进行通信的线程组件
 * @author machi
 *
 */
public class NameNodeServiceActor {

	private static final String NAMENODE_HOSTNAME = "localhost";
	private static final Integer NAMENODE_PORT = 50070;

	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

	public NameNodeServiceActor() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}

	/**
	 * 向自己负责通信的那个NameNode进行注册
	 */
	public void register() throws InterruptedException {
		Thread registerThread = new RegisterThread();
		registerThread.start();
		registerThread.join();
	}


	// 向NameNode进行心跳
	public void startHeartbeat() {
		new HeartbeatThread().start();
	}

	/**
	 * 负责注册的线程
	 * @author machi
	 *
	 */
	class RegisterThread extends Thread {
		
		@Override
		public void run() {
			try {
				// 发送rpc接口调用请求到NameNode去进行注册
				System.out.println("发送RPC请求到NameNode进行注册.......");  

				String ip = "127.0.0.1";
				String hostname = "dfs-data-01";
				// 通过RPC接口发送到NameNode他的注册接口上去
				RegisterRequest registerRequest = RegisterRequest.newBuilder()
						.setIp(ip)
						.setHostname(hostname)
						.build();
				RegisterResponse response = namenode.register(registerRequest);

				System.out.println("收到namenode返回的注册响应:"+response.getStatus());


			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
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
