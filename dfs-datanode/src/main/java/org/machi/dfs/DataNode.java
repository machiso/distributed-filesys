package org.machi.dfs;

import java.io.File;
import java.util.Arrays;

public class DataNode {

	private volatile Boolean shouldRun;

	private NameNodeRpcClient nameNodeRpcClient;

	public DataNode() {
		this.shouldRun = true;
		this.nameNodeRpcClient = new NameNodeRpcClient();
		if (nameNodeRpcClient.register()){
			nameNodeRpcClient.startHeartbeat();

			StorageInfo storageInfo = getStorageInfo();
			if (storageInfo != null){
				//全量上报
				nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
			}

			DataNodeNIOServer nioServer = new DataNodeNIOServer(nameNodeRpcClient);
			nioServer.start();
		}else {
			System.out.println("向NameNode注册失败，直接退出......");
			System.exit(1);
		}
	}

	/**
	 * 获取存储信息
	 * @return
	 */
	private StorageInfo getStorageInfo() {
		StorageInfo storageInfo = new StorageInfo();

		File file = new File(DataNodeConfig.DATA_DIR);
		if (!file.exists()){
			return null;
		}
		File[] children = file.listFiles();
		if (children == null || children.length == 0){
			return null;
		}

		Arrays.stream(children).forEach(child -> scanfile(child,storageInfo));

		return storageInfo;
	}

	//扫描文件
	private void scanfile(File dir,StorageInfo storageInfo) {
		if (dir.isFile()){
			String path = dir.getPath();
			path = path.substring(DataNodeConfig.DATA_DIR.length() - 2);
			// \image\product\iphone.jpg
			path = path.replace("\\\\", "/"); // /image/product/iphone.jpg

			storageInfo.addFilename(path);
			storageInfo.addStoredDataSize(dir.length());

			return;
		}
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
