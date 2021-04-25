package org.machi.dfs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhss.dfs.namenode.rpc.model.*;
import io.grpc.stub.StreamObserver;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * NameNode的rpc服务的接口
 * @author machi
 *
 */
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

	public static final Integer STATUS_SUCCESS = 1;
	public static final Integer STATUS_FAILURE = 2;

	private static final int BACKUP_NODE_FETCH_SIZE = 10;

	/**
	 * 负责管理元数据的核心组件
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private DataNodeManager datanodeManager;

	//当前backupNode节点同步到了哪一条txid了
	private long syncedTxid = 0L;

	//当前缓存里的editslog最大的一个txid
	private long currentBufferedMaxTxid = 0L;

	//当前内存里缓冲了哪个磁盘文件的数据
	private String bufferedFlushedTxid;

	//当前缓冲的一小部分editslog
	private JSONArray currentBufferedEditsLog = new JSONArray();

	public NameNodeServiceImpl(
			FSNamesystem namesystem,
			DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
		this.datanodeManager = datanodeManager;
	}


	@Override
	public void register(RegisterRequest request,
						 StreamObserver<RegisterResponse> responseObserver) {
		datanodeManager.register(request.getIp(), request.getHostname());

		RegisterResponse response = RegisterResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


	@Override
	public void heartbeat(HeartbeatRequest request,
						  StreamObserver<HeartbeatResponse> responseObserver) {
		datanodeManager.heartbeat(request.getIp(), request.getHostname());

		HeartbeatResponse response = HeartbeatResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


	@Override
	public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
		try {
			this.namesystem.mkdir(request.getPath());

			System.out.println("创建目录：path" + request.getPath());

			MkdirResponse response = MkdirResponse.newBuilder()
					.setStatus(STATUS_SUCCESS)
					.build();

			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {

	}

	/**
	 * backupnode来批量抓取editsLog
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
		FetchEditsLogResponse response = null;
		JSONArray fetchedEditsLog = new JSONArray();

		List<String> flushedTxids = namesystem.getEditsLog().getFlushedTxids();
		//如果当前还没有刷入磁盘到数据，那么都在内存中
		if (flushedTxids.size() == 0){
			fetchFromBufferedEditsLog(fetchedEditsLog);
		}
		//如果此时发现已经有落地磁盘的文件了，这个时候就要扫描所有的磁盘文件的索引范围
		else {
			// 第一种情况，你要拉取的txid是在某个磁盘文件里的
			// 有磁盘文件，而且内存里还缓存了某个磁盘文件的数据了
			if (bufferedFlushedTxid != null){
				// 如果要拉取的数据就在当前缓存的磁盘文件数据里
				if (existInFlushedFile(bufferedFlushedTxid)){
					fetchFromCurrentBuffer(fetchedEditsLog);
				}
				// 如果要拉取的数据不在当前缓存的磁盘文件数据里了，那么需要从下一个磁盘文件去拉取
				else {
					String nextFlushedTxid = getNextFlushedTxid(flushedTxids, bufferedFlushedTxid);
					if (nextFlushedTxid != null){
						fetchFromFlushedFile(nextFlushedTxid,fetchedEditsLog);
					}
					// 如果没有找到下一个文件，此时就需要从内存里去继续读取
					else {
						fetchFromBufferedEditsLog(fetchedEditsLog);
					}
				}
			}
			//第一次尝试从磁盘文件里去拉取
			else {
				Boolean fechedFromFlushedFile = false;
				for (String flushedTxid : flushedTxids){
					if (existInFlushedFile(flushedTxid)){
						System.out.println("尝试从磁盘文件中拉取editslog，flushedTxid=" + flushedTxid);
						// 此时可以把这个磁盘文件里以及下一个磁盘文件的的数据都读取出来，放到内存里来缓存
						// 就怕一个磁盘文件的数据不足够10条
						fetchFromFlushedFile(flushedTxid, fetchedEditsLog);
						fechedFromFlushedFile = true;
						break;
					}
				}
				// 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
				// 如果没有找到下一个文件，此时就需要从内存里去继续读取
				if (!fechedFromFlushedFile){
					fetchFromBufferedEditsLog(fetchedEditsLog);
				}
			}
		}

		response = FetchEditsLogResponse.newBuilder()
				.setEditsLog(fetchedEditsLog.toJSONString())
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	private String getNextFlushedTxid(List<String> flushedTxids, String bufferedFlushedTxid) {
		for(int i = 0; i < flushedTxids.size(); i++) {
			if(flushedTxids.get(i).equals(bufferedFlushedTxid)) {
				if(i + 1 < flushedTxids.size()) {
					return flushedTxids.get(i + 1);
				}
			}
		}
		return null;
	}

	private void fetchFromFlushedFile(String flushedTxid, JSONArray fetchedEditsLog) {
		try {
			String[] flushedTxidSplited = flushedTxid.split("_");
			long startTxid = Long.valueOf(flushedTxidSplited[0]);
			long endTxid = Long.valueOf(flushedTxidSplited[1]);

			String currentEditsLogFile = "F:\\development\\editslog\\edits-"
					+ startTxid + "-" + endTxid + ".log";
			List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile),
					StandardCharsets.UTF_8);

			currentBufferedEditsLog.clear();
			for(String editsLog : editsLogs) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
				currentBufferedMaxTxid = JSONObject.parseObject(editsLog).getLongValue("txid");
			}
			bufferedFlushedTxid = flushedTxid; // 缓存了某个刷入磁盘文件的数据

			fetchFromCurrentBuffer(fetchedEditsLog);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//是否存在于刷到磁盘的文件中
	private boolean existInFlushedFile(String flushedTxid) {
		String[] flushedTxidSplited = flushedTxid.split("_");
		if (Long.valueOf(flushedTxidSplited[0]) <= syncedTxid + 1 && Long.valueOf(flushedTxidSplited[0]) >= syncedTxid){
			return true;
		}
		return false;
	}

	//从当前内存中来获取editslog
	private void fetchFromBufferedEditsLog(JSONArray fetchedEditsLog) {
		long fetchTxid = syncedTxid + 1;
		if (fetchTxid <= currentBufferedMaxTxid){
			fetchFromCurrentBuffer(fetchedEditsLog);
			return;
		}
		currentBufferedEditsLog.clear();

		//从内存中再重新拉取一次
		String[] bufferedEditsLog = namesystem.getEditsLog().getBufferedEditsLog();

		if (bufferedEditsLog != null){
			Arrays.stream(bufferedEditsLog).forEach(editLog -> {
				currentBufferedEditsLog.add(JSONObject.parseObject(editLog));
				//当前内存缓存中最大的txid
				currentBufferedMaxTxid = JSONObject.parseObject(editLog).getLong("txid");
			});
			bufferedFlushedTxid = null;

			fetchFromCurrentBuffer(fetchedEditsLog);
		}
	}

	//从当前已经存在到内存缓冲中来获取数据
	private void fetchFromCurrentBuffer(JSONArray fetchedEditsLog) {
		int fetchCount = 0;
		for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
			if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") == syncedTxid + 1) {
				fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
				syncedTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
				fetchCount++;
			}
			if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
				break;
			}
		}
	}

}
