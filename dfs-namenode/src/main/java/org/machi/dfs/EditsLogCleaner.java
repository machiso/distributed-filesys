package org.machi.dfs;

import java.io.File;
import java.util.List;

/**
 * 定时清理editglog日志
 * @author machi
 */
public class EditsLogCleaner extends Thread {

    private static long EDITSLOG_CLEANER_INTERVAL = 60 * 1000;

    private FSNamesystem fsNamesystem;

    public EditsLogCleaner(FSNamesystem fsNamesystem) {
        this.fsNamesystem = fsNamesystem;
    }

    @Override
    public void run() {
        System.out.println("editlog日志文件后台清理线程启动......");
        while (true){
            try {
                Thread.sleep(EDITSLOG_CLEANER_INTERVAL);
                //拿到fsimage中的最大txid，之前的日志都可以删除了
                long txid = fsNamesystem.getFsImageCheckPointTxid();
                deleteEditsLog(txid);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void deleteEditsLog(long fsImageTxid) {
        List<String> flushedTxids = fsNamesystem.getEditsLog().getFlushedTxids();
        for (String flushTxid : flushedTxids){
            long fluedId = Long.valueOf(flushTxid.split("-")[1]);
            if (fluedId <= fsImageTxid){
                String path = "/Users/machi/edits/edits-" + flushTxid + ".log";;
                deleteFile(path);
            }
        }
    }

    public void deleteFile(String path){
        File file = new File(path);
        if (file.exists())
            file.delete();
    }
}
