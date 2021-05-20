package org.machi.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.machi.dfs.DataNodeConfig.*;

public class DataNodeNIOServer extends Thread {

	// NIO的selector，负责多路复用监听多个连接的请求
	private Selector selector;
	// 内存队列，无界队列
	private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<LinkedBlockingQueue<SelectionKey>>();
	// 缓存的没读取完的文件数据
	private Map<String, CachedImage> cachedImages = new HashMap<String, CachedImage>();
	// 与NameNode进行通信的客户端
	private NameNodeRpcClient namenodeRpcClient;

	/**
	 * NIOServer的初始化，监听端口、队列初始化、线程初始化
	 */
	public DataNodeNIOServer(NameNodeRpcClient namenodeRpcClient){
		ServerSocketChannel serverSocketChannel = null;

		try {
			this.namenodeRpcClient = namenodeRpcClient;

			selector = Selector.open();

			serverSocketChannel = ServerSocketChannel.open();
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.socket().bind(new InetSocketAddress(NIO_PORT), 100);
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

			for(int i = 0; i < 3; i++) {
				queues.add(new LinkedBlockingQueue<SelectionKey>());
			}

			for(int i = 0; i < 3; i++) {
				new Worker(queues.get(i)).start();
			}

			System.out.println("DataNode NIOServer已经启动，开始监听端口：" + NIO_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		/**
		 * 无限循环，等待IO多路复用方式监听请求
		 */
		while(true){
			try{
				selector.select();
				Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

				while(keysIterator.hasNext()){
					SelectionKey key = (SelectionKey) keysIterator.next();
					keysIterator.remove();
					handleRequest(key);
				}
			}
			catch(Throwable t){
				t.printStackTrace();
			}
		}
	}

	/**
	 * 处理请求分发
	 * @param key
	 * @throws IOException
	 * @throws ClosedChannelException
	 */
	private void handleRequest(SelectionKey key)
			throws IOException, ClosedChannelException {
		SocketChannel channel = null;

		try{
			if(key.isAcceptable()){
				ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
				channel = serverSocketChannel.accept();
				if(channel != null) {
					channel.configureBlocking(false);
					channel.register(selector, SelectionKey.OP_READ);
				}
			}
			else if(key.isReadable()){
				channel = (SocketChannel) key.channel();
				String remoteAddr = channel.getRemoteAddress().toString();
				int queueIndex = remoteAddr.hashCode() % queues.size();
				queues.get(queueIndex).put(key);
			}
		}
		catch(Throwable t){
			t.printStackTrace();
			if(channel != null){
				channel.close();
			}
		}
	}

	class Worker extends Thread {

		private LinkedBlockingQueue<SelectionKey> queue;

		public Worker(LinkedBlockingQueue<SelectionKey> queue) {
			this.queue = queue;
		}

		@Override
		public void run() {
			while(true) {
				SocketChannel channel = null;

				try {
					SelectionKey key = queue.take();

					channel = (SocketChannel) key.channel();
					if(!channel.isOpen()) {
						channel.close();
						continue;
					}
					String remoteAddr = channel.getRemoteAddress().toString();
					System.out.println("接收到客户端的请求：" + remoteAddr);

					ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
					// 从请求中解析文件名
					// 已经是：F:\\development\\tmp1\\image\\product\\iphone.jpg
					Filename filename = getFilename(channel, buffer);
					System.out.println("从网络请求中解析出来文件名：" + filename);
					if(filename == null) {
						channel.close();
						continue;
					}
					// 从请求中解析文件大小
					long imageLength = getImageLength(channel, buffer);
					System.out.println("从网络请求中解析出来文件大小：" + imageLength);
					// 定义已经读取的文件大小
					long hasReadImageLength = getHasReadImageLength(channel);
					System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);

					// 构建针对本地文件的输出流
					FileOutputStream imageOut = new FileOutputStream(filename.absoluteFilename);
					FileChannel imageChannel = imageOut.getChannel();
					imageChannel.position(imageChannel.size());

					// 如果是第一次接收到请求，就应该把buffer里剩余的数据写入到文件里去
					if(!cachedImages.containsKey(remoteAddr)) {
						hasReadImageLength += imageChannel.write(buffer);
						System.out.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
						buffer.clear();
					}

					// 循环不断的从channel里读取数据，并写入磁盘文件
					int len = -1;
					while((len = channel.read(buffer)) > 0) {
						hasReadImageLength += len;
						System.out.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
						buffer.flip();
						imageChannel.write(buffer);
						buffer.clear();
					}
					imageChannel.close();
					imageOut.close();

					// 判断一下，如果已经读取完毕，就返回一个成功给客户端
					if(hasReadImageLength == imageLength) {
						ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
						channel.write(outBuffer);
						cachedImages.remove(remoteAddr);
						System.out.println("文件读取完毕，返回响应给客户端: " + remoteAddr);

						// 增量上报Master节点自己接收到了一个文件的副本
						// /image/product/iphone.jpg
						namenodeRpcClient.informReplicaReceived(filename.relativeFilename);
					}
					// 如果一个文件没有读完，缓存起来，等待下一次读取
					else {
						CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
						cachedImages.put(remoteAddr, cachedImage);
						key.interestOps(SelectionKey.OP_READ);
						System.out.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件：" + cachedImage);
					}
				} catch (Exception e) {
					e.printStackTrace();
					if(channel != null) {
						try {
							channel.close();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}

	}

	/**
	 * 从网络请求中获取文件名
	 * @param channel
	 * @param buffer
	 * @return
	 * @throws Exception
	 */
	private Filename getFilename(SocketChannel channel, ByteBuffer buffer) throws Exception {
		Filename filename = new Filename();
		String remoteAddr = channel.getRemoteAddress().toString();

		if(cachedImages.containsKey(remoteAddr)) {
			filename = cachedImages.get(remoteAddr).filename;
		} else {
			String relativeFilename = getRelativeFilename(channel, buffer);
			if(relativeFilename == null) {
				return null;
			}
			// /image/product/iphone.jpg
			filename.relativeFilename = relativeFilename;

			String[] relativeFilenameSplited = relativeFilename.split("/");

			String dirPath = DATA_DIR;
			for(int i = 0; i < relativeFilenameSplited.length - 1; i++) {
				if(i == 0) {
					continue;
				}
				dirPath += "\\" + relativeFilenameSplited[i];
			}

			File dir = new File(dirPath);
			if(!dir.exists()) {
				dir.mkdirs();
			}

			String absoluteFilename = dirPath + "\\" +
					relativeFilenameSplited[relativeFilenameSplited.length - 1];
			filename.absoluteFilename = absoluteFilename;
		}

		return filename;
	}

	/**
	 * 从网络请求中获取相对文件名
	 * @param channel
	 * @param buffer
	 * @return
	 */
	private String getRelativeFilename(SocketChannel channel, ByteBuffer buffer) throws Exception {
		int len = channel.read(buffer); // position就往前推动了，必须得先flip一下，让position回到0
		if(len > 0) {
			buffer.flip();

			byte[] filenameLengthBytes = new byte[4];
			buffer.get(filenameLengthBytes, 0, 4);

			ByteBuffer filenameLengthBuffer = ByteBuffer.allocate(4);
			filenameLengthBuffer.put(filenameLengthBytes);
			filenameLengthBuffer.flip();
			int filenameLength = filenameLengthBuffer.getInt();

			byte[] filenameBytes = new byte[filenameLength];
			buffer.get(filenameBytes, 0, filenameLength);
			String filename = new String(filenameBytes); // 这里返回的应该就是：/image/product/iphone.jpg

			return filename;
		}

		return null;
	}

	/**
	 * 从网络请求中获取文件大小
	 * @param channel
	 * @param buffer
	 * @return
	 * @throws Exception
	 */
	private Long getImageLength(SocketChannel channel, ByteBuffer buffer) throws Exception {
		Long imageLength = 0L;
		String remoteAddr = channel.getRemoteAddress().toString();

		if(cachedImages.containsKey(remoteAddr)) {
			imageLength = cachedImages.get(remoteAddr).imageLength;
		} else {
			byte[] imageLengthBytes = new byte[8];
			buffer.get(imageLengthBytes, 0, 8);

			ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
			imageLengthBuffer.put(imageLengthBytes);
			imageLengthBuffer.flip();
			imageLength = imageLengthBuffer.getLong();
		}

		return imageLength;
	}

	/**
	 * 获取已经读取的文件大小
	 * @param channel
	 * @return
	 * @throws Exception
	 */
	private Long getHasReadImageLength(SocketChannel channel) throws Exception {
		long hasReadImageLength = 0;
		String remoteAddr = channel.getRemoteAddress().toString();
		if(cachedImages.containsKey(remoteAddr)) {
			hasReadImageLength = cachedImages.get(remoteAddr).hasReadImageLength;
		}
		return hasReadImageLength;
	}


	class Filename {

		// 相对路径名
		String relativeFilename;
		// 绝对路径名
		String absoluteFilename;

		@Override
		public String toString() {
			return "Filename [relativeFilename=" + relativeFilename + ", absoluteFilename=" + absoluteFilename + "]";
		}

	}

	/**
	 * 缓存好的文件
	 * @author zhonghuashishan
	 *
	 */
	class CachedImage {

		Filename filename;
		long imageLength;
		long hasReadImageLength;

		public CachedImage(Filename filename, long imageLength, long hasReadImageLength) {
			this.filename = filename;
			this.imageLength = imageLength;
			this.hasReadImageLength = hasReadImageLength;
		}

		@Override
		public String toString() {
			return "CachedImage [filename=" + filename + ", imageLength=" + imageLength + ", hasReadImageLength="
					+ hasReadImageLength + "]";
		}

	}

}
