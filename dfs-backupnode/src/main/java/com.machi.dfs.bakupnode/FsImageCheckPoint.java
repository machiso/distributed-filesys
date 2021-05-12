package com.machi.dfs.bakupnode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * checkpoint检查机制
 * @author machi
 */
public class FsImageCheckPoint extends Thread{

    public static final Integer CHECK_POINT_INTEVAL = 2 * 60 * 1000;

    //上一次的fsImage文件
    private String lastFsImageFile = "";

    private BackupNode backupNode;
    private FSNamesystem namesystem;
    private NameNodeRpcClient nameNodeRpcClient;

    public FsImageCheckPoint(BackupNode backupNode,FSNamesystem namesystem,NameNodeRpcClient nameNodeRpcClient) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    @Override
    public void run() {
        System.out.println("FSImageCheckPoint 定时调度线程启动");
        while (backupNode.isRunning()){
            try {
                if (!namesystem.isFinishedRecover()){
                    System.out.println("当前还没完成元数据恢复，不进行checkpoint......");
                    Thread.sleep(1000);
                    continue;
                }

                if(lastFsImageFile.equals("")) {
                    this.lastFsImageFile = namesystem.getCheckpointFile();
                }

                long checkPointTime = namesystem.getCheckPointTime();
                long now = System.currentTimeMillis();
                if (now - checkPointTime > CHECK_POINT_INTEVAL){
                    System.out.println("准备执行checkpoint操作，写入fsimage文件......");
                    doCheckPoint();
                    System.out.println("完成checkpoint操作......");
                }
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void doCheckPoint() throws Exception {
        //这里就基于文件目录树，来做一个check point的检查
        FSImage fsImage = namesystem.getFSImage();

        //删除旧的磁盘文件
        deleteLastCheckPoint();

        //将fsimage文件刷入本地磁盘
        writeFSImageFile(fsImage);

        //将fsimage文件上传server端
        uploadFSImageFile(fsImage);

        //将fsimage中的最大txid通知server
        updateCheckpointTxid(fsImage);

        //持久化checkpoint相关信息
        saveChekckPointInfo(fsImage);
    }

    private void saveChekckPointInfo(FSImage fsImage) {
        String fsImageInfoPath = "/Users/machi/edits/backup/checkpoint-info.meta";
        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel channel = null;

        try {
            file = new RandomAccessFile(fsImageInfoPath, "rw"); // 读写模式，数据写入缓冲区中
            out = new FileOutputStream(file.getFD());
            channel = out.getChannel();

            long checkPointTime = System.currentTimeMillis();
            ByteBuffer buffer = ByteBuffer.wrap((checkPointTime + "_" +fsImage.getMaxTxid()).getBytes());

            channel.write(buffer);
            channel.force(false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(out != null) {
                    out.close();
                }
                if(file != null) {
                    file.close();
                }
                if(channel != null) {
                    channel.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void updateCheckpointTxid(FSImage fsImage) {
        nameNodeRpcClient.updateCheckpointTxid(fsImage.getMaxTxid());
    }

    private void uploadFSImageFile(FSImage fsImage) {
        FSImageUploadClient fsImageUploadClient = new FSImageUploadClient(fsImage);
        fsImageUploadClient.start();
    }

    private void deleteLastCheckPoint() throws Exception{
        File file = new File(lastFsImageFile);
        if (file.exists()){
            file.delete();
        }
    }

    private void writeFSImageFile(FSImage fsImage) throws Exception{
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsImageJson().getBytes());

        String fsImageFilePath = "/Users/machi/edits/backup/fsimage-"
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
