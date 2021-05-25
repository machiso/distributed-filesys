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
							buffer.putInt(SEND_FILE); //请求类型

							buffer.putInt(filenameBytes.length); // 文件名长度
							buffer.put(filenameBytes); // 文件名

							buffer.putLong(fileSize); // 文件大小
							buffer.put(file); //真正的文件内容

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

						byte[] filenameBytes = filename.getBytes();

						// int（4个字节）int（4个字节）文件名（N个字节）
						ByteBuffer readFileRequest = ByteBuffer.allocate(8 + filenameBytes.length);
						readFileRequest.putInt(READ_FILE); //请求的类型
						readFileRequest.putInt(filenameBytes.length); // 先放入4个字节的int，是一个数字，527，,336，代表了这里的文件名有多少个字节
						readFileRequest.put(filenameBytes); // 再把真正的文件名给放入进去
						readFileRequest.flip();

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
