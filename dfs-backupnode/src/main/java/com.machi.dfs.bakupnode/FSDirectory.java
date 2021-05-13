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
	private INode dirTree;

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
		this.dirTree = new INode("/");  
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

		try {
			writeLock();

			maxTxid = txid;

			String[] pathes = path.split("/");
			INode parent = dirTree;
			
			for(String splitedPath : pathes) {
				if(splitedPath.trim().equals("")) {
					continue;
				}
				
				INode dir = findDirectory(parent, splitedPath);
				if(dir != null) {
					parent = dir;
					continue;
				}
				
				INode child = new INode(splitedPath); 
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

	private void printDirTree(INode dirTree, String blank) {
		if(dirTree.getChildren().size() == 0) {
			return;
		}
		for(INode dir : dirTree.getChildren()) {
			System.out.println(blank + ((INode) dir).getPath());
			printDirTree((INode) dir, blank + " ");
		}
	}
	
	/**
	 * 对文件目录树递归查找目录
	 * @param dir
	 * @param path
	 * @return
	 */
	private INode findDirectory(INode dir, String path) {
		if(dir.getChildren().size() == 0) {
			return null;
		}

		for(INode child : dir.getChildren()) {
			if(child instanceof INode) {
				INode childDir = (INode) child;
				if((childDir.getPath().equals(path))) {
					return childDir;
				}
			}
		}

		return null;
	}

	public boolean createFile(String filename) {
		try {
			writeLock();

			String[] splitedFileName = filename.split("/");
			String fileRealPath = splitedFileName[splitedFileName.length - 1];
			INode parentTree = dirTree;

			for(String splitedPath : splitedFileName) {
				if(splitedPath.trim().equals("")) {
					continue;
				}

				INode dir = findDirectory(parentTree, splitedPath);
				if(dir != null) {
					parentTree = dir;
					continue;
				}

				INode child = new INode(splitedPath);
				parentTree.addChild(child);

				parentTree = child;
			}

			if (existFile(parentTree,fileRealPath)){
				return false;
			}

			INode iNode = new INode(fileRealPath);
			parentTree.addChild(iNode);
			return true;
		}finally {
			writeUnLock();
		}
	}

	private boolean existFile(INode parent, String fileRealPath) {
		if (parent.getChildren() != null && parent.getChildren().size() > 0){
			List<INode> iNodes = parent.getChildren();
			for (INode node : iNodes){
				if (fileRealPath.equals(node.getPath())){
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 代表文件目录树中的一个目录
	 * @author machi
	 *
	 */
	public static class INode {
		
		private String path;
		private List<INode> children;
		
		public INode(String path) {
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

	public long getMaxTxid() {
		return maxTxid;
	}

	public void setMaxTxid(long maxTxid) {
		this.maxTxid = maxTxid;
	}

	public INode getDirTree() {
		return dirTree;
	}

	public void setDirTree(INode dirTree) {
		this.dirTree = dirTree;
	}
}
