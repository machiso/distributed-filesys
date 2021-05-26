package org.machi.dfs;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.machi.dfs.DataNodeConfig.DATA_DIR;
import static org.machi.dfs.DataNodeConfig.NIO_PORT;


/**
 * 数据节点的NIOServer
 * @author zhonghuashishan
 *
 */
public class DataNodeNIOServer extends Thread {

	public static final Integer SEND_FILE = 1;
	public static final Integer READ_FILE = 2;
	public static final Integer NIO_BUFFER_SIZE = 10 * 1024;

	// NIO的selector，负责多路复用监听多个连接的请求
	private Selector selector;
	// 内存队列，无界队列
	private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<LinkedBlockingQueue<SelectionKey>>();
	// 缓存没读取完的文件数据
	private Map<String, CachedRequest> cachedRequests = new ConcurrentHashMap<String, CachedRequest>();
	// 缓存没读取完的请求类型
	private Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<String, ByteBuffer>();
	// 缓存没读取完的文件名大小
	private Map<String, ByteBuffer> filenameLengthByClient = new ConcurrentHashMap<String, ByteBuffer>();
	// 缓存没读取完的文件名
	private Map<String, ByteBuffer> filenameByClient = new ConcurrentHashMap<String, ByteBuffer>();
	// 缓存没读取完的文件大小
	private Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<String, ByteBuffer>();
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

			System.out.println("NIOServer已经启动，开始监听端口：" + NIO_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while(true){
			try{
				selector.select();
				Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

				while(keysIterator.hasNext()){
					SelectionKey key = (SelectionKey) keysIterator.next();
					keysIterator.remove();
					handleEvents(key);
				}
			}
			catch(Throwable t){
				t.printStackTrace();
			}
		}
	}

	private void handleEvents(SelectionKey key)
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
				channel = (SocketChannel)key.channel();
				String client = channel.getRemoteAddress().toString();
				int queueIndex = client.hashCode() % queues.size();
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
					handleRequest(channel, key);
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
	 * 处理客户端发送过来的请求
	 * @param channel
	 * @param key
	 * @throws Exception
	 */
	private void handleRequest(SocketChannel channel, SelectionKey key) throws Exception {
		// 假如说你这个一次读取的数据里包含了多个文件的话
		// 这个时候我们会先读取文件名，然后根据文件的大小去读取这么多的数据
		String remoteAddr = channel.getRemoteAddress().toString();
		System.out.println("接收到客户端的请求：" + remoteAddr);

		// 需要先提取出来这次请求是什么类型：1 发送文件；2 读取文件
		if(cachedRequests.containsKey(remoteAddr)) {
			handleSendFileRequest(channel, key);
		} else {
			Integer requestType = getRequestType(channel); // 但是此时channel的position肯定也变为了4

			if(requestType == null) {
				return;
			}

			// 拆包，就是说人家一次请求，本来是包含了：requestType + filenameLength + filename [+ imageLength + image]
			// 这次OP_READ事件，就读取到了requestType的4个字节中的2个字节，剩余的数据
			// 就被放在了下一次OP_READ事件中了

			if(SEND_FILE.equals(requestType)) {
				handleSendFileRequest(channel, key);
			} else if(READ_FILE.equals(requestType)) {
				handleReadFileRequest(channel, key);
			}
		}
	}

	/**
	 * 获取本次请求的类型
	 * @param channel
	 * @return
	 */
	public Integer getRequestType(SocketChannel channel) throws Exception {
		Integer requestType = null;
		String client = channel.getRemoteAddress().toString();

		if(getCachedRequest(client).reqeustType != null) {
			return getCachedRequest(client).reqeustType;
		}

		ByteBuffer requestTypeBuffer = null;
		if(requestTypeByClient.containsKey(client)) {
			requestTypeBuffer = requestTypeByClient.get(client);
		} else {
			requestTypeBuffer = ByteBuffer.allocate(4);
		}

		channel.read(requestTypeBuffer);  // 此时requestType ByteBuffer，position跟limit都是4，remaining是0

		if(!requestTypeBuffer.hasRemaining()) {
			// 已经读取出来了4个字节，可以提取出来requestType了
			requestTypeBuffer.rewind(); // 将position变为0，limit还是维持着4
			requestType = requestTypeBuffer.getInt();
			System.out.println("从请求中解析出来本次请求的类型：" + requestType);

			requestTypeByClient.remove(client);

			CachedRequest cachedRequest = getCachedRequest(client);
			cachedRequest.reqeustType = requestType;
		} else {
			requestTypeByClient.put(client, requestTypeBuffer);
		}

		return requestType;
	}

	/**
	 * 获取缓存的请求
	 * @param client
	 * @return
	 */
	private CachedRequest getCachedRequest(String client) {
		CachedRequest cachedRequest = cachedRequests.get(client);
		if(cachedRequest == null) {
			cachedRequests.put(client, new CachedRequest());
		}
		return cachedRequest;
	}

	/**
	 * 获取文件名同时转换为本地磁盘目录中的绝对路径
	 * @param channel
	 * @return
	 * @throws Exception
	 */
	private Filename getFilename(SocketChannel channel) throws Exception {
		Filename filename = new Filename();
		String client = channel.getRemoteAddress().toString();

		if(getCachedRequest(client).filename != null) {
			return getCachedRequest(client).filename;
		} else {
			String relativeFilename = getRelativeFilename(channel);
			if(relativeFilename == null) {
				return null;
			}
			// /image/product/iphone.jpg
			filename.relativeFilename = relativeFilename;

			String absoluteFilename = getAbsoluteFilename(relativeFilename);
			filename.absoluteFilename = absoluteFilename;

			CachedRequest cachedRequest = getCachedRequest(client);
			cachedRequest.filename = filename;
		}

		return filename;
	}

	/**
	 * 获取相对路径的文件名
	 * @param channel
	 * @return
	 */
	private String getRelativeFilename(SocketChannel channel) throws Exception {
		String client = channel.getRemoteAddress().toString();

		Integer filenameLength = null;
		String filename = null;

		// 读取文件名的大小
		if(!filenameByClient.containsKey(client)) {
			ByteBuffer filenameLengthBuffer = null;
			if(filenameLengthByClient.containsKey(client)) {
				filenameLengthBuffer = filenameLengthByClient.get(client);
			} else {
				filenameLengthBuffer = ByteBuffer.allocate(4);
			}

			channel.read(filenameLengthBuffer);

			if(!filenameLengthBuffer.hasRemaining()) {
				filenameLengthBuffer.rewind();
				filenameLength = filenameLengthBuffer.getInt();
				filenameLengthByClient.remove(client);
			} else {
				filenameLengthByClient.put(client, filenameLengthBuffer);
			}
		}

		// 读取文件名
		ByteBuffer filenameBuffer = null;
		if(filenameByClient.containsKey(client)) {
			filenameBuffer = filenameByClient.get(client);
		} else {
			filenameBuffer = ByteBuffer.allocate(filenameLength);
		}

		channel.read(filenameBuffer);

		if(!filenameBuffer.hasRemaining()) {
			filenameBuffer.rewind();
			filename = new String(filenameBuffer.array());
			filenameByClient.remove(client);
		} else {
			filenameByClient.put(client, filenameBuffer);
		}

		return filename;
	}

	/**
	 * 获取文件在本地磁盘上的绝对路径名
	 * @param relativeFilename
	 * @return
	 * @throws Exception
	 */
	private String getAbsoluteFilename(String relativeFilename) throws Exception {
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
			return null;
		}

		String absoluteFilename = dirPath + "\\" +
				relativeFilenameSplited[relativeFilenameSplited.length - 1];
		return absoluteFilename;
	}

	/**
	 * 从网络请求中获取文件大小
	 * @param channel
	 * @return
	 * @throws Exception
	 */
	private Long getFileLength(SocketChannel channel) throws Exception {
		Long fileLength = null;
		String client = channel.getRemoteAddress().toString();

		if(getCachedRequest(client).fileLength != null) {
			return getCachedRequest(client).fileLength;
		} else {
			ByteBuffer fileLengthBuffer = ByteBuffer.allocate(8);
			channel.read(fileLengthBuffer);
			if(!fileLengthBuffer.hasRemaining()) {
				fileLength = fileLengthBuffer.getLong();
				fileLengthByClient.remove(client);
				getCachedRequest(client).fileLength = fileLength;
			} else {
				fileLengthByClient.put(client, fileLengthBuffer);
			}
		}

		return fileLength;
	}

	private void handleSendFileRequest(SocketChannel channel, SelectionKey key) throws Exception {
		String remoteAddr = channel.getRemoteAddress().toString();

		Filename filename = getFilename(channel);
		System.out.println("从网络请求中解析出来文件名：" + filename);
		if(filename == null) {
			return;
		}
		// 从请求中解析文件大小
		Long fileLength = getFileLength(channel);
		System.out.println("从网络请求中解析出来文件大小：" + fileLength);
		if(fileLength == null) {
			return;
		}
		// 定义已经读取的文件大小
		long hasReadImageLength = getHasReadFileLength(channel);
		System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);

		// 构建针对本地文件的输出流
		FileOutputStream imageOut = new FileOutputStream(filename.absoluteFilename);
		FileChannel imageChannel = imageOut.getChannel();
		imageChannel.position(imageChannel.size());

		// 循环不断的从channel里读取数据，并写入磁盘文件
		ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
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
		if(hasReadImageLength == fileLength) {
			ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
			channel.write(outBuffer);
			cachedRequests.remove(remoteAddr);
			System.out.println("文件读取完毕，返回响应给客户端: " + remoteAddr);

			// 增量上报Master节点自己接收到了一个文件的副本
			// /image/product/iphone.jpg
			namenodeRpcClient.informReplicaReceived(filename.relativeFilename);
			System.out.println("增量上报收到的文件副本给NameNode节点......");

			key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
		}
	}

	private void handleReadFileRequest(SocketChannel channel, SelectionKey key) throws Exception {
		String remoteAddr = channel.getRemoteAddress().toString();

		// 从请求中解析文件名
		// 已经是：F:\\development\\tmp1\\image\\product\\iphone.jpg
		Filename filename = getFilename(channel);
		System.out.println("从网络请求中解析出来文件名：" + filename);
		if(filename == null) {
			return;
		}

		File file = new File(filename.absoluteFilename);
		Long fileLength = file.length();

		FileInputStream imageIn = new FileInputStream(filename.absoluteFilename);
		FileChannel imageChannel = imageIn.getChannel();

		// 循环不断的从channel里读取数据，并写入磁盘文件
		ByteBuffer buffer = ByteBuffer.allocate(
				8 + Integer.valueOf(String.valueOf(fileLength)));
		buffer.putLong(fileLength);
		int hasReadImageLength = imageChannel.read(buffer);

		buffer.rewind();
		channel.write(buffer);

		imageChannel.close();
		imageIn.close();

		// 判断一下，如果已经读取完毕，就返回一个成功给客户端
		if(hasReadImageLength == fileLength) {
			System.out.println("文件发送完毕，给客户端: " + remoteAddr);
			key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
		}
	}

	/**
	 * 获取已经读取的文件大小
	 * @param channel
	 * @return
	 * @throws Exception
	 */
	private Long getHasReadFileLength(SocketChannel channel) throws Exception {
		String client = channel.getRemoteAddress().toString();
		if(getCachedRequest(client).hasReadFileLength != null) {
			return getCachedRequest(client).hasReadFileLength;
		}
		return 0L;
	}

	/**
	 * 文件名
	 * @author zhonghuashishan
	 *
	 */
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
	 * 缓存文件
	 * @author zhonghuashishan
	 *
	 */
	class CachedRequest {

		Integer reqeustType;
		Filename filename;
		Long fileLength;
		Long hasReadFileLength;

	}

}
