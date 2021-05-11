package org.machi.dfs;

import java.util.LinkedList;
import java.util.List;

/**
 * 负责管理内存中的文件目录树的核心组件
 * @author machi
 */
public class FSDirectory {
	
	/**
	 * 内存中的文件目录树
	 */
	private INode dirTree;
	
	public FSDirectory() {
		this.dirTree = new INode("/");
	}

	public INode getDirTree() {
		return dirTree;
	}

	public void setDirTree(INode dirTree) {
		this.dirTree = dirTree;
	}

	/**
	 * 创建目录
	 * @param path 目录路径
	 */
	public void mkdir(String path) {

		//这里需要采用锁将整个文件内存目录树给加锁，保证内存数据的数据一致性
		synchronized(dirTree) {
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
	
	/**
	 * 代表文件目录树中的一个目录
	 * @author machi
	 *
	 */
	public class INode {
		
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
}
