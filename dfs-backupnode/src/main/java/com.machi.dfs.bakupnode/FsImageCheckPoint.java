package com.machi.dfs.bakupnode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author machi
 */
public class FsImageCheckPoint extends Thread{

    public static final Integer CHECK_POINT_INTEVAL = 10 * 1000;

    //上一次的fsImage文件
    private String lastFsImageFile = "";

    private BackupNode backupNode;
    private FSNamesystem namesystem;

    public FsImageCheckPoint(BackupNode backupNode,FSNamesystem namesystem) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
    }

    @Override
    public void run() {
        System.out.println("FSImageCheckPoint 定时调度线程启动");
        while (backupNode.isRunning()){
            try {
                Thread.sleep(CHECK_POINT_INTEVAL);

                //这里就基于文件目录树，来做一个check point的检查
                FSImage fsImage = namesystem.getFSImage();

                //删除旧的磁盘文件
                deleteLastCheckPoint();

                //将fsimage文件刷入磁盘
                doCheckPoint(fsImage);

            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void deleteLastCheckPoint() throws Exception{
        File file = new File(lastFsImageFile);
        if (file.exists()){
            file.delete();
        }
    }

    private void doCheckPoint(FSImage fsImage) throws Exception{
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsImageJson().getBytes());

        String fsImageFilePath = "/Users/machi/edits/fsimage-"
                + fsImage.getMaxTxid() + ".meta";

        lastFsImageFile = fsImageFilePath;

        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel channel = null;

        try {
            file = new RandomAccessFile(fsImageFilePath, "rw"); // 读写模式，数据写入缓冲区中
            out = new FileOutputStream(file.getFD());
            channel = out.getChannel();

            channel.write(buffer);
            channel.force(false);  // 强制把数据刷入磁盘上
        } finally {
            if(out != null) {
                out.close();
            }
            if(file != null) {
                file.close();
            }
            if(channel != null) {
                channel.close();
            }
        }
    }
}
