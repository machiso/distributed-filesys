package org.machi.dfs;

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
		this.editlog = new FSEditlog();
	}
	
	//创建目录
	//首先需要维护内存中的文件目录树（也叫命名空间）
	//接着需要构造一条editlog，用来记录内存中做了哪些操作，editlog存入内存中，采用内存双缓存机制
	//异步将内存数据刷入磁盘中
	//虽然mkdir和logEdit都使用了重量级同步机制synchronized，但是由于没有设计到磁盘和网络IO操作，完全基于内存来进行
	//速度其实是很快的
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
