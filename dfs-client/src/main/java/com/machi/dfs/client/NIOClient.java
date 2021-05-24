package com.machi.dfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NIOClient {

	public static final Integer SEND_FILE = 1;
	public static final Integer READ_FILE = 2;

	public void sendFile(String hostname, int nioPort,
						 byte[] file, String filename, long fileSize) {
		// 建立一个短连接，发送完一个文件就释放网络连接
		SocketChannel channel = null;
		Selector selector = null;

		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(new InetSocketAddress(hostname, nioPort));
			selector = Selector.open();
			channel.register(selector, SelectionKey.OP_CONNECT);

			boolean sending = true;

			while(sending){
				selector.select();

				Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
				while(keysIterator.hasNext()){
					SelectionKey key = (SelectionKey) keysIterator.next();
					keysIterator.remove();

					// NIOServer允许进行连接的话
					if(key.isConnectable()){
						channel = (SocketChannel) key.channel();

						if(channel.isConnectionPending()){
							channel.finishConnect(); // 把三次握手做完，TCP连接建立好了

							// 封装文件的请求数据
							byte[] filenameBytes = filename.getBytes();

							ByteBuffer buffer = ByteBuffer.allocate((int)fileSize * 2 + filenameBytes.length);
							buffer.putInt(SEND_FILE);
							buffer.putInt(filenameBytes.length); // 先放入4个字节的int，是一个数字，527，,336，代表了这里的文件名有多少个字节

							buffer.put(filenameBytes); // 再把真正的文件名给放入进去

							buffer.putLong(fileSize); // long对应了8个字节，放到buffer里去
							buffer.put(file);

							//这里一定要flip一下，才能将数据发送出去
							//这里buffer分配的字节数比较大，如果不执行flip的话，会将你剩余的空白数据发送出去
							//flip之后会将之前塞入的数据发送
							buffer.flip();

							int sentData = channel.write(buffer);
							System.out.println("已经发送了" + sentData + "字节的数据到" + hostname);

							channel.register(selector, SelectionKey.OP_READ);
						}
					}
					// 接收到NIOServer的响应
					else if(key.isReadable()){
						channel = (SocketChannel) key.channel();

						ByteBuffer buffer = ByteBuffer.allocate(1024);
						int len = channel.read(buffer);

						if(len > 0) {
							System.out.println("[" + Thread.currentThread().getName()
									+ "]收到" + hostname + "的响应：" + new String(buffer.array(), 0, len));
							sending = false;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(channel != null){
				try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if(selector != null){
				try {
					selector.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 读取文件
	 * @param hostname 数据节点的hostname
	 * @param nioPort 数据节点的nio端口号
	 * @param filename 文件名
	 */
	public void readFile(String hostname, int nioPort, String filename) {
		SocketChannel channel = null;
		Selector selector = null;

		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(new InetSocketAddress(hostname, nioPort));

			selector = Selector.open();
			channel.register(selector, SelectionKey.OP_CONNECT);

			boolean reading = true;

			while(reading){
				selector.select();

				Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
				while(keysIterator.hasNext()){
					SelectionKey key = (SelectionKey) keysIterator.next();
					keysIterator.remove();

					// NIOServer允许进行连接的话
					if(key.isConnectable()){
						channel = (SocketChannel) key.channel();

						if(channel.isConnectionPending()){
							channel.finishConnect(); // 把三次握手做完，TCP连接建立好了
						}

						// 在这里，第一步，一旦建立起来一个连接，直接就是发送一个请求过去
						// 意思就是说，你想要读取一个文件
						// 其实你就应该先发送你这个请求要做的事情，比如用Integer类型来进行代表，4个字节，int数字
						// 1：发送文件过去；2：读取文件过来
						// 2+文件名的字节数+实际的文件名
						// 客户端发送完请求之后，立马就是关注OP_READ事件，他要等着去读取人家发送过来的数据
						// 一旦说读取完毕了文件，再次关注OP_WRITE，发送一个SUCCESS过去给人家

						// 服务端而言，先读取开头的4个字节，判断一下 你要干什么
						// 如果是1，发送文件，就转入之前写的那套代码；如果是2，读取文件，那么就新写一套逻辑
						// 人家就需要解析出来你的文件名，转换为他的本地存储的路径，把文件读取出来，给你发送过来
						// 一旦发送完毕了文件之后，就关注OP_READ事件，等待读取人家发送过来的结果，SUCCESS

						// 最后都完毕之后，双方都要去断开各自的连接

						byte[] filenameBytes = filename.getBytes();

						// int（4个字节）int（4个字节）文件名（N个字节）

						ByteBuffer readFileRequest = ByteBuffer.allocate(8 + filenameBytes.length);
						readFileRequest.putInt(READ_FILE);
						readFileRequest.putInt(filenameBytes.length); // 先放入4个字节的int，是一个数字，527，,336，代表了这里的文件名有多少个字节
						readFileRequest.put(filenameBytes); // 再把真正的文件名给放入进去
						readFileRequest.flip();

						// 如果大家去跟着我大数据的课，kafka源码剖析的课去看的话，就会知道
						// kafka里面是用了一模一样的方式，基于一个预定义好的二进制规范，按照规范往ByteBuffer里
						// 写入几个字节的size，几个字节的数据

						int sentData = channel.write(readFileRequest);
						System.out.println("已经发送了" + sentData + "字节的数据到" + hostname + "机器的" + nioPort + "端口上");

						channel.register(selector, SelectionKey.OP_READ);
					}
					// 接收到NIOServer的响应
					else if(key.isReadable()){
						channel = (SocketChannel) key.channel();

						ByteBuffer buffer = ByteBuffer.allocate(1024);
						int len = channel.read(buffer);

						if(len > 0) {
							System.out.println("[" + Thread.currentThread().getName()
									+ "]收到" + hostname + "的响应：" + new String(buffer.array(), 0, len));
							reading = false;
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(channel != null){
				try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if(selector != null){
				try {
					selector.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
