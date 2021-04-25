package org.machi.dfs;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 内存双缓冲
 * @author machi
 *
 */
class DoubleBuffer {

    //单块editslog缓冲区的最大大小：默认是512字节
    public static final Integer EDIT_LOG_BUFFER_LIMIT = 512 * 1024;

    //是专门用来承载线程写入edits log
    EditLogBuffer currentBuffer = new EditLogBuffer();

    //专门用来将数据同步到磁盘中去的一块缓冲
    EditLogBuffer syncBuffer = new EditLogBuffer();

    /**
     * 当前这块缓冲区写入的最大的一个txid
     */
    private long startTxid = 1L;

    /**
     * 已经输入磁盘中的txid范围
     * todo 如果刷入的磁盘txid越来越多，这边需要进行优化
     */
    private List<String> flushedTxids = new ArrayList<String>();

    //当前写入的最大的txid
    public Long maxTxid;


    //将edits log写到内存缓冲里去
    public void write(EditLog log) throws IOException {
        currentBuffer.write(log);
        this.maxTxid = log.txid;
    }

    //交换两块缓冲区，为了同步内存数据到磁盘做准备
    public void setReadyToSync() {
        EditLogBuffer tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    //判断一下当前的缓冲区是否写满了需要刷到磁盘上去
    public boolean shouldSyncToDisk() {
        if(currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT) {
            return true;
        }
        return false;
    }

    /**
     * 将syncBuffer缓冲区中的数据刷入磁盘中
     */
    public void flush() throws IOException {
        syncBuffer.flush();
        syncBuffer.clear();
    }


    /**
     * 获取当前缓冲区里的数据
     * @return
     */
    public String[] getBufferedEditsLog() {
        if (currentBuffer.size() == 0){
            return null;
        }
        String editsLogRawData = new String(currentBuffer.getBufferData());
        return editsLogRawData.split("\n");
    }

    //返回已经刷入磁盘的txid editsLog数据
    public List<String> getFlushedTxids() {
        return flushedTxids;
    }


    //内存缓冲区，代表具体缓冲区的一个类
    public class EditLogBuffer{

        ByteArrayOutputStream buffer ;

        /**
         * 上一次flush到磁盘的时候他的最大的txid是多少
         */
        long endTxid = 0L;

        public EditLogBuffer() {
            this.buffer = new ByteArrayOutputStream(EDIT_LOG_BUFFER_LIMIT*2);
        }

        //写入edit log
        public void write(EditLog log) throws IOException {
            buffer.write(log.getContent().getBytes());
            buffer.write("\n".getBytes());
            System.out.println("当前缓冲区大小："+currentBuffer.size());
        }

        //获取当前缓冲区已经写入数据的字节数量
        public Integer size() {
            return buffer.size();
        }

        public void flush() throws IOException {
            byte[] data = buffer.toByteArray();
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);

            String editsLogFilePath = "F:\\development\\editslog\\edits-"
                    + startTxid + "-" + endTxid + ".log";
            flushedTxids.add(startTxid + "_" + endTxid);

            RandomAccessFile file = null;
            FileOutputStream out = null;
            FileChannel editsLogFileChannel = null;

            try {
                file = new RandomAccessFile(editsLogFilePath, "rw"); // 读写模式，数据写入缓冲区中
                out = new FileOutputStream(file.getFD());
                editsLogFileChannel = out.getChannel();

                editsLogFileChannel.write(dataBuffer);
                editsLogFileChannel.force(false);  // 强制把数据刷入磁盘上
            } finally {
                if(out != null) {
                    out.close();
                }
                if(file != null) {
                    file.close();
                }
                if(editsLogFileChannel != null) {
                    editsLogFileChannel.close();
                }
            }

            startTxid = endTxid + 1;
        }

        public void clear() {

        }

        //获取当前内存缓冲区中的字节数组
        public byte[] getBufferData() {
            return buffer.toByteArray();
        }
    }

}
