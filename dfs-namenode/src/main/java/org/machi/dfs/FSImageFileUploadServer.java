package org.machi.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * namenode 接受 backupnode发送fsimage文件的server端
 * @author machi
 */
public class FSImageFileUploadServer extends Thread{

    private Selector selector;

    public FSImageFileUploadServer() {
        this.init();
    }

    private void init(){
        ServerSocketChannel serverSocketChannel = null;

        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
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

    private void handleRequest(SelectionKey key) {
        try{
            if(key.isAcceptable()){
                handleAcceptRequest(key);
            } else if(key.isReadable()){
                handleReadableRequest(key);
            } else if(key.isWritable()) {
               handleWriableRequest(key);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    private void handleWriableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put("SUCCESS".getBytes());
        buffer.flip();

        channel = (SocketChannel) key.channel();
        channel.write(buffer);
        channel.register(selector, SelectionKey.OP_READ);
    }

    private void handleReadableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            //删除之前一次的fsimage文件
            String fsImageFilePath = "/Users/machi/edits/namenode-fsimage.meta";

            //接受client发送的同步fsimage的请求，并且写入磁盘
            RandomAccessFile fsimageImageRAF = null;
            FileOutputStream fsimageOut = null;
            FileChannel fsimageFileChannel = null;

            try {
                channel = (SocketChannel) key.channel();
                ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
                int total = 0;
                int count = -1;

                if ((count = channel.read(buffer)) > 0){
                    //第一次请求的时候，就将上一次的fsimage文件删除
                    File file = new File(fsImageFilePath);
                    if (file.exists()){
                        file.delete();
                    }

                    fsimageImageRAF = new RandomAccessFile(fsImageFilePath, "rw");
                    fsimageOut = new FileOutputStream(fsimageImageRAF.getFD());
                    fsimageFileChannel = fsimageOut.getChannel();

                    total += count;

                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                }else {
                    //后面会进行空轮询，如果从buffer中获取不到信息，那么就可以将channel关闭
                    channel.close();
                }

                while ((count = channel.read(buffer)) > 0){
                    total += count;
                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                }

                if(total > 0) {
                    System.out.println("接收fsimage文件以及写入本地磁盘完毕......");
                    fsimageFileChannel.force(false);
                    channel.register(selector, SelectionKey.OP_WRITE);
                }

            } finally {
                if(fsimageOut != null) {
                    fsimageOut.close();
                }
                if(fsimageImageRAF != null) {
                    fsimageImageRAF.close();
                }
                if(fsimageFileChannel != null) {
                    fsimageFileChannel.close();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            if (channel !=null){
                channel.close();
            }
        }
    }

    private void handleAcceptRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        channel = serverSocketChannel.accept();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
    }
}
