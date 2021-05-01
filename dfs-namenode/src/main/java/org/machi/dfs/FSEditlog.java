package org.machi.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责管理edits log日志的核心组件
 * @author machi
 *
 */
public class FSEditlog {

	/**
	 * 当前递增到的txid的序号
	 */
	private long txidSeq = 0L;

	/**
	 * 当前是否在将内存缓冲刷入磁盘中,默认为false
	 */
	private volatile Boolean isSyncRunning = false;

	//是否正在调度一次刷盘的操作，默认false
	private volatile Boolean isSchedulingSync = false;

	//内存双缓冲
	private DoubleBuffer doubleBuffer = new DoubleBuffer();

	//已经刷入磁盘的txid文件
	private List<String> flushedTxids = new ArrayList<>();
	/**
	 * 在同步到磁盘中的最大的一个txid
	 */
	private volatile Long syncTxid = 0L;
	/**
	 * 每个线程自己本地的txid副本
	 */
	private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

	//记录edits log日志
	//首先判断当前是否在刷盘，如果是的话，那么直接阻塞等待，如果没有在刷盘，构造一条editlog对象，
	//并且判断当前缓冲区大小是否已经到达指定的内存大小，没到达的话，直接return,到达的话则需要进行刷盘操作
	//刷盘：将刷盘的标志位更新为true
	public void logEdit(String content) {

		// 这里必须得直接加锁
		synchronized(this) {
			//首先要判断一下当前editlog是否在进行同步磁盘的操作。如果是的话，需要阻塞等待
			isSyncRunning();

			// 获取全局唯一递增的txid，代表了edits log的序号
			txidSeq++;
			long txid = txidSeq;
			localTxid.set(txid); // 放到ThreadLocal里去，相当于就是维护了一份本地线程的副本

			// 构造一条edits log对象
			EditLog log = new EditLog(txid, content);

			// 将edits log写入内存缓冲中，不是直接刷入磁盘文件
			//todo 如果写入内存缓冲区失败，这边应该如何处理；另外，到底是应该先写入再判断大小还是先判断大小，再写入呢？
			try {
				doubleBuffer.write(log);
			} catch (IOException e) {
				e.printStackTrace();
			}

			//判断当前缓冲区是否已经超过了内存大小，超过的话需要将缓冲区中的editlog刷磁盘
			if (!doubleBuffer.shouldSyncToDisk()){
				return;
			}

			//将同步磁盘标志位更新位true，别的线程进入logEdit方法中，首先就会阻塞
			//走到这里，代表当前的缓冲区已满，再来log的话已经放不下了，需要调度一次刷磁盘的操作，因此请求到这里都会被卡住
			isSchedulingSync = true;
		}

		logSync();
	}


	//判断当前是否正在刷磁盘，如果是，需要阻塞等待1s
	private void isSyncRunning() {
		while (isSchedulingSync){
			try {
				wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 将内存缓冲中的数据刷入磁盘文件中
	 * 在这里尝试允许某一个线程一次性将内存缓冲中的数据刷入磁盘文件中
	 * 相当于实现一个批量将内存缓冲数据刷磁盘的过程
	 */
	private void logSync(){
		// 再次尝试加锁
		synchronized(this) {
			long txid = localTxid.get(); // 获取到本地线程的副本
			// 如果说当前正好有人在刷内存缓冲到磁盘中去
			if(isSyncRunning) {
				if(txid <= syncTxid) {
					return;
				}

				while(isSyncRunning) {
					try {
						wait(2000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			// 交换两块缓冲区
			doubleBuffer.setReadyToSync();

			syncTxid = txid;

			isSchedulingSync = false;
			notifyAll();
			// 设置当前正在同步到磁盘的标志位
			isSyncRunning = true;
		}

		// 开始同步内存缓冲的数据到磁盘文件里去
		// 这个过程其实是比较慢，基本上肯定是毫秒级了，弄不好就要几十毫秒
		try {
			doubleBuffer.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}

		synchronized(this) {
			// 同步完了磁盘之后，就会将标志位复位，再释放锁
			isSyncRunning = false;
			// 唤醒可能正在等待他同步完磁盘的线程
			notifyAll();
		}
	}

	/**
	 * 获取已经刷入到磁盘editslog数据
	 * @return
	 */
	public List<String> getFlushedTxids() {
		//使用读写锁，避免这里出现阻塞等待
//		synchronized (this){
		return doubleBuffer.getFlushedTxids();
//		}
	}

	//获取当前缓存currentBuffer中的数据
	public String[] getBufferedEditsLog() {
		synchronized (this){
			return doubleBuffer.getBufferedEditsLog();
		}
	}

	//将缓存中的数据刷入磁盘文件中
	public void flush() {
		try {
			doubleBuffer.setReadyToSync();
			doubleBuffer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
