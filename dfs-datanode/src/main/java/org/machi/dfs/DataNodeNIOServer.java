package org.machi.dfs;

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
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class DataNodeNIOServer extends Thread {

    private Selector selector;
    private List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<LinkedBlockingQueue<SelectionKey>>();
    private Map<String, CachedImage> cachedImages = new HashMap<String, CachedImage>();
    
    class CachedImage {
    	
    	String filename;
    	long imageLength;
    	long hasReadImageLength;
    	
		public CachedImage(String filename, long imageLength, long hasReadImageLength) {
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
       
    public DataNodeNIOServer(){
        ServerSocketChannel serverSocketChannel = null;

        try {
        	selector = Selector.open();
        	
        	serverSocketChannel = ServerSocketChannel.open();
        	serverSocketChannel.configureBlocking(false);
        	serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            
            for(int i = 0; i < 3; i++) {
            	queues.add(new LinkedBlockingQueue<SelectionKey>());
            }
            
            for(int i = 0; i < 3; i++) {
            	new Worker(queues.get(i)).start();
            }
            
            System.out.println("NIOServer已经启动，开始监听端口：" + 9000);
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
                    handleRequest(key);
                }
            }
            catch(Throwable t){
                t.printStackTrace();
            }
        }
    }
	
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
					
	                ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
	                int len = -1;
	                
	                String filename = null;
	                if(cachedImages.containsKey(remoteAddr)) {
	                	filename = cachedImages.get(remoteAddr).filename;
	                } else {
	                	filename = "F:\\development\\tmp\\" + UUID.randomUUID().toString() + ".jpg";
	                }
	                
	        		long imageLength = 0;
	        		
	        		if(cachedImages.containsKey(remoteAddr)) {
	        			imageLength = cachedImages.get(remoteAddr).imageLength;
	        		} else {
	        			len = channel.read(buffer);
		        		buffer.flip();
		        		if(len > 8) {
		        			byte[] imageLengthBytes = new byte[8];
		        			buffer.get(imageLengthBytes, 0, 8);
		        			
		        			ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
		        			imageLengthBuffer.put(imageLengthBytes);
		        			imageLengthBuffer.flip();
		        			imageLength = imageLengthBuffer.getLong();
		        		} else if(len <= 0) {
		        			channel.close(); 
		        			continue;
		        		}
	        		}
	        		
	        		long hasReadImageLength = 0;
	        		if(cachedImages.containsKey(remoteAddr)) {
	        			hasReadImageLength = cachedImages.get(remoteAddr).hasReadImageLength;
	        		}
	        		
	        		FileOutputStream imageOut = new FileOutputStream(filename);   
		        	FileChannel imageChannel = imageOut.getChannel();
		        	imageChannel.position(imageChannel.size());
	        		
	        		if(!cachedImages.containsKey(remoteAddr)) {
		        		hasReadImageLength += imageChannel.write(buffer);
		        		buffer.clear();
	        		}
	        		
	                while((len = channel.read(buffer)) > 0) {
	                	hasReadImageLength += len;
	                	buffer.flip();
	                	imageChannel.write(buffer);
	                	buffer.clear();
	                }
	                
	                if(cachedImages.get(remoteAddr) != null) {
	                	if(hasReadImageLength == cachedImages.get(remoteAddr).hasReadImageLength) {
		                	channel.close();
		                	continue;
		                }
	                }
	                
	                imageChannel.close();
	                imageOut.close();
	                
	                if(hasReadImageLength == imageLength) {
	                	ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
	                	channel.write(outBuffer);
	                	cachedImages.remove(remoteAddr);
	                } else {
	                	CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
	                	cachedImages.put(remoteAddr, cachedImage);  
	                	key.interestOps(SelectionKey.OP_READ);
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
	
}
