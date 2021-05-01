package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONObject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 负责管理内存中的文件目录树的核心组件
 * @author machi
 *
 */
public class FSDirectory {
	
	/**
	 * 内存中的文件目录树
	 */
	private INodeDirectory dirTree;

	/**
	 *	读写锁
	 */
	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	//当前editslog
	private long maxTxid = 0L;

	public void writeLock(){
		lock.writeLock().lock();
	}

	public void writeUnLock(){
		lock.writeLock().unlock();
	}

	public void readLock(){
		lock.readLock().lock();
	}

	public void readUnlock(){
		lock.readLock().unlock();
	}
	
	public FSDirectory() {
		this.dirTree = new INodeDirectory("/");  
	}

	//用json的格式获取内存中的元数据
	public FSImage getFSImageByJson(){
		FSImage fsImage;
		try {
			readLock();
			fsImage = new FSImage(maxTxid,JSONObject.toJSONString(dirTree));
		}finally {
			readUnlock();
		}
		return fsImage;
	}
	
	//创建目录
	//这里采用读写锁来保证并发安全性，主要是创建目录和check point检查点之间，一个要对文件目录树进行添加，一个要对文件目录树进行读取
	public void mkdir(long txid,String path) {
		// path = /usr/warehouse/hive
		// 你应该先判断一下，“/”根目录下有没有一个“usr”目录的存在
		// 如果说有的话，那么再判断一下，“/usr”目录下，有没有一个“/warehouse”目录的存在
		// 如果说没有，那么就得先创建一个“/warehosue”对应的目录，挂在“/usr”目录下
		// 接着再对“/hive”这个目录创建一个节点挂载上去


		//这里需要采用锁将整个文件内存目录树给加锁，保证内存数据的数据一致性
		try {
			writeLock();

			maxTxid = txid;

			String[] pathes = path.split("/");
			INodeDirectory parent = dirTree;
			
			for(String splitedPath : pathes) {
				if(splitedPath.trim().equals("")) {
					continue;
				}
				
				INodeDirectory dir = findDirectory(parent, splitedPath);
				if(dir != null) {
					parent = dir;
					continue;
				}
				
				INodeDirectory child = new INodeDirectory(splitedPath); 
				parent.addChild(child);

				parent = child;
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			writeUnLock();
		}
//		printDirTree(dirTree, "");
	}

	private void printDirTree(INodeDirectory dirTree, String blank) {
		if(dirTree.getChildren().size() == 0) {
			return;
		}
		for(INode dir : dirTree.getChildren()) {
			System.out.println(blank + ((INodeDirectory) dir).getPath());
			printDirTree((INodeDirectory) dir, blank + " ");
		}
	}
	
	/**
	 * 对文件目录树递归查找目录
	 * @param dir
	 * @param path
	 * @return
	 */
	private INodeDirectory findDirectory(INodeDirectory dir, String path) {
		if(dir.getChildren().size() == 0) {
			return null;
		}

		for(INode child : dir.getChildren()) {
			if(child instanceof INodeDirectory) {
				INodeDirectory childDir = (INodeDirectory) child;
				if((childDir.getPath().equals(path))) {
					return childDir;
				}
			}
		}

		return null;
	}
	
	
	/**
	 * 代表的是文件目录树中的一个节点
	 * @author machi
	 *
	 */
	private interface INode {
		
	}
	
	/**
	 * 代表文件目录树中的一个目录
	 * @author machi
	 *
	 */
	public static class INodeDirectory implements INode {
		
		private String path;
		private List<INode> children;
		
		public INodeDirectory(String path) {
			this.path = path;
			this.children = new LinkedList<INode>();
		}
		
		public void addChild(INode inode) {
			this.children.add(inode);
		}
		
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public List<INode> getChildren() {
			return children;
		}
		public void setChildren(List<INode> children) {
			this.children = children;
		}
		
	}
	
	/**
	 * 代表文件目录树中的一个文件
	 * @author machi
	 *
	 */
	public static class INodeFile implements INode {
		
		private String name;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		
	}
	
}
