package org.machi.dfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
		recoverNameSpace();
	}

	//namenode启动的时候进行数据恢复到内存中
	private void recoverNameSpace() {

		String path = "/Users/machi/edits/namenode-fsimage.meta";
		try {
			File file = new File(path);
			if (!file.exists()){
				return;
			}
			//将fsimage加载到内存中
			loadFsImageFile(path);
			//将fsimage中到最大txid从磁盘文件加载出来
			loadCheckPointTxid();
			//将editlog加载到内存中
			loadEditLogFromFile();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	private void loadCheckPointTxid() {
		String path = "/Users/machi/edits/namenode/checkpoint-txid.meta";
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			in = new FileInputStream(path);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024);
			int total = channel.read(buffer);
			buffer.flip();

			String fsimageJson = new String(buffer.array(),0,total);
			long txid = Long.valueOf(fsimageJson);
			System.out.println("恢复fsimage txid：" + txid);

			this.fsImageCheckPointTxid = fsImageCheckPointTxid;
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

	private void loadEditLogFromFile() throws Exception{
		String path = "/Users/machi/edits";
		File file = new File(path);
		File[] files = file.listFiles();

		List<File> editsFiles = new ArrayList<>();
		for (File s : files){
			if (s.getName().contains("edits")){
				editsFiles.add(s);
			}
		}
		//对文件名进行排序
		Collections.sort(editsFiles, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				Integer o1Name = Integer.valueOf(o1.getName().split("-")[1]);
				Integer o2Name = Integer.valueOf(o2.getName().split("-")[1]);
				return o1Name - o2Name;
			}
		});

		if (editsFiles == null || editsFiles.size() == 0){
			return;
		}

		for (File editLog : editsFiles){
			if (editLog.getName().contains("edits")){
				String[] splitedName = file.getName().split("-");
				long startTxid = Long.valueOf(splitedName[1]);
				long endTxid = Long.valueOf(splitedName[2].split("[.]")[0]);

				if (fsImageCheckPointTxid < endTxid){
					String editsLogPath = "/Users/machi/edits/edits-" + startTxid + "-" + endTxid + ".log";
					List<String> list = Files.readAllLines(Paths.get(editsLogPath), StandardCharsets.UTF_8);
					list.stream().forEach(log -> {
						JSONObject jsonObject = JSONObject.parseObject(log);
						long txid = jsonObject.getLong("txid");
						if (txid > fsImageCheckPointTxid){
							System.out.println("准备回放editlog：" + log);
							String op = jsonObject.getString("OP");
							if ("MKDIR".equals(op)){
								String paths = jsonObject.getString("PATH");
								directory.mkdir(paths);
							}
						}
					});
				}
			}
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

    //将txid同步到磁盘文件
	public void saveCheckPointTxid(long fsImageCheckPointTxid) {
		String path = "/Users/machi/edits/namenode/checkpoint-txid.meta";

		RandomAccessFile raf = null;
		FileOutputStream out = null;
		FileChannel channel = null;
		try {
			File file = new File(path);
			if (file.exists()){
				file.delete();
			}
			channel = out.getChannel();
			raf = new RandomAccessFile(path, "rw");
			out = new FileOutputStream(raf.getFD());

			ByteBuffer buffer = ByteBuffer.wrap(String.valueOf(fsImageCheckPointTxid).getBytes());

			channel.write(buffer);
			channel.force(false);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
				if (raf != null){
					raf.close();
				}
				if (out != null){
					out.close();
				}
				if (channel != null){
					channel.close();
				}
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}
}
