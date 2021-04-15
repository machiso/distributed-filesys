package org.machi.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
    public void flush() {
        syncBuffer.flush();
        syncBuffer.clear();
    }


    //内存缓冲区，代表具体缓冲区的一个类
    public class EditLogBuffer{

        ByteArrayOutputStream outputStream ;

        public EditLogBuffer() {
            this.outputStream = new ByteArrayOutputStream(EDIT_LOG_BUFFER_LIMIT*2);
        }

        //写入edit log
        public void write(EditLog log) throws IOException {
            outputStream.write(log.getContent().getBytes());
            outputStream.write("\n".getBytes());
            System.out.println("当前缓冲区大小："+currentBuffer.size());
        }

        //获取当前缓冲区已经写入数据的字节数量
        public Integer size() {
            return outputStream.size();
        }

        public void flush() {

        }

        public void clear() {

        }
    }

}
