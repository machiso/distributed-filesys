package com.machi.dfs.bakupnode;

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
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
	}
	
	//创建目录
	//首先需要维护内存中的文件目录树（也叫命名空间）
	//接着需要构造一条editlog，用来记录内存中做了哪些操作，editlog存入内存中，采用内存双缓存机制
	//异步将内存数据刷入磁盘中
	//虽然mkdir和logEdit都使用了重量级同步机制synchronized，但是由于没有设计到磁盘和网络IO操作，完全基于内存来进行
	//速度其实是很快的
	public Boolean mkdir(long txid,String path){
		this.directory.mkdir(txid,path);
		return true;
	}

	//获取文件目录树json
	public FSImage getFSImage() throws Exception{
		return directory.getFSImageByJson();
	}

}
