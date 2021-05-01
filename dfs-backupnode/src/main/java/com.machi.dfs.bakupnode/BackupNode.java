package com.machi.dfs.bakupnode;

/**
 * 负责同步editlog的进程
 * @author machi
 */
public class BackupNode {

    private volatile Boolean isRunning = true;

    private FSNamesystem fsNamesystem;

    public static void main(String[] args) throws Exception{
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
    }

    public void init(){
        fsNamesystem = new FSNamesystem();
    }

    public void start(){
        EditsLogFetcher fetcher = new EditsLogFetcher(this,fsNamesystem);
        fetcher.start();

        FsImageCheckPoint fsImageCheckPoint = new FsImageCheckPoint(this,fsNamesystem);
        fsImageCheckPoint.start();
    }

    public boolean isRunning() {
        return isRunning;
    }
}
