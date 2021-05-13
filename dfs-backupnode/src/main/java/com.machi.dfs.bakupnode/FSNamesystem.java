package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 负责管理元数据的核心组件
 * @author machi
 *
 */
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	private FSDirectory directory;

	//backupnode是否完成重启
	private volatile boolean finishedRecover = false;

	//checkpoint时间
	private long checkPointTime;
	//checkpoint txid
	private long syncedTxid;

	private String checkpointFile = "";
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		//恢复元数据
		recoverNamespace();
	}

	/**
	 * 1、加载checkpoint相关信息到内存中
	 * 2、加载fsimage文件到内存
	 */
	private void recoverNamespace() {
		loadCheckPointInfo();
		loadFsImageFile();
		finishedRecover = true;
	}

	private void loadFsImageFile() {
		String fsImageInfoPath = "/Users/machi/edits/backup/fsimage-" + syncedTxid + ".meta";
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			File file = new File(fsImageInfoPath);
			if (!file.exists()){
				System.out.println("fsimage文件不存在，不进行恢复.......");
				return;
			}

			in = new FileInputStream(fsImageInfoPath);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
			int count = channel.read(buffer);
			buffer.flip();

			String fsImageJson = new String(buffer.array(),0,count);
			FSDirectory.INode node = JSONObject.parseObject(fsImageJson, FSDirectory.INode.class);
			directory.setDirTree(node);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
				if (in != null){
					in.close();
				}
				if (channel != null){
					channel.close();
				}
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	private void loadCheckPointInfo() {
		String checkPointInfoPath = "/Users/machi/edits/backup/checkpoint-info.meta";
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			File file = new File(checkPointInfoPath);
			if (!file.exists()){
				System.out.println("checkpoint info文件不存在，不进行恢复.......");
				return;
			}

			in = new FileInputStream(checkPointInfoPath);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024);
			int count = channel.read(buffer);
			buffer.flip();

			String checkPointInfo = new String(buffer.array(),0,count);
			long checkPointTime = Long.valueOf(checkPointInfo.split("_")[0]);
			long syncedTxid = Long.valueOf(checkPointInfo.split("_")[1]);
			this.checkPointTime = checkPointTime;
			this.syncedTxid = syncedTxid;
			directory.setMaxTxid(syncedTxid);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
				if (in != null){
					in.close();
				}
				if (channel != null){
					channel.close();
				}
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	public Boolean mkdir(long txid,String path){
		this.directory.mkdir(txid,path);
		return true;
	}

	//获取文件目录树json
	public FSImage getFSImage(){
		return directory.getFSImageByJson();
	}

	public boolean isFinishedRecover() {
		return finishedRecover;
	}

	public void setFinishedRecover(boolean finishedRecover) {
		this.finishedRecover = finishedRecover;
	}

	public long getCheckPointTime() {
		return checkPointTime;
	}

	public void setCheckPointTime(long checkPointTime) {
		this.checkPointTime = checkPointTime;
	}

	public String getCheckpointFile() {
		return checkpointFile;
	}

	public void setCheckpointFile(String checkpointFile) {
		this.checkpointFile = checkpointFile;
	}

	public void createFile(String filename) {
		directory.createFile(filename);
	}
}
