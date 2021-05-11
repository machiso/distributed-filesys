package org.machi.dfs;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editlog;

	//最近一次fsimage文件的最大txid
	private long fsImageCheckPointTxid;
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog(this);
		recoverFsImageFile();
	}

	//namenode启动的时候进行fsimage恢复到内存中
	private void recoverFsImageFile() {

		String path = "/Users/machi/edits/namenode-fsimage.meta";
		try {
			File file = new File(path);
			if (!file.exists()){
				return;
			}
			//将磁盘文件加载到内存中
			loadFsImageFile(path);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	private void loadFsImageFile(String path) throws IOException {
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			in = new FileInputStream(path);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
			int count = channel.read(buffer);
			buffer.flip();

			String fsimageJson = new String(buffer.array(),0,count);
			System.out.println("恢复fsimage文件中的数据：" + fsimageJson);

			FSDirectory.INode node = JSONObject.parseObject(fsimageJson, FSDirectory.INode.class);
			directory.setDirTree(node);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			if (in != null){
				in.close();
			}
			if (channel != null){
				channel.close();
			}
		}
	}

	//创建目录
	public Boolean mkdir(String path){
		this.directory.mkdir(path); 
		this.editlog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");
		return true;
	}

	/**
	 * 获取一个EditsLog组件
	 * @return
	 */
    public FSEditlog getEditsLog() {
		return editlog;
    }

	/**
	 * 强制刷盘
	 */
	public void flush() {
		this.editlog.flush();
	}

	public long getFsImageCheckPointTxid() {
		return fsImageCheckPointTxid;
	}

	public void setCheckpointTxid(long txid) {
		this.fsImageCheckPointTxid = txid;
    }
}
