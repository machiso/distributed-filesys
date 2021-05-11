package com.machi.dfs.bakupnode;

/**
 * 负责同步editlog的进程
 * @author machi
 */
public class BackupNode {

    private volatile Boolean isRunning = true;

    private FSNamesystem fsNamesystem;

    private NameNodeRpcClient nameNodeRpcClient;

    public static void main(String[] args) throws Exception{
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
    }

    public void init(){
        fsNamesystem = new FSNamesystem();
        nameNodeRpcClient = new NameNodeRpcClient();
    }

    public void start(){
        EditsLogFetcher fetcher = new EditsLogFetcher(this,fsNamesystem,nameNodeRpcClient);
        fetcher.start();

        FsImageCheckPoint fsImageCheckPoint = new FsImageCheckPoint(this,fsNamesystem,nameNodeRpcClient);
        fsImageCheckPoint.start();
    }

    public boolean isRunning() {
        return isRunning;
    }
}
