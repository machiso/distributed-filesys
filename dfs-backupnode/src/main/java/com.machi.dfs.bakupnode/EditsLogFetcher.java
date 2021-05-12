package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 抓取editslog
 * @author machi
 */
public class EditsLogFetcher extends Thread{

    private BackupNode backupNode;

    private FSNamesystem namesystem;

    private NameNodeRpcClient nameNodeRpcClient;


    public EditsLogFetcher(BackupNode backupNode, FSNamesystem fsNamesystem,NameNodeRpcClient nameNodeRpcClient) {
        this.backupNode = backupNode;
        this.namesystem = fsNamesystem;
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    @Override
    public void run() {
        while (backupNode.isRunning()){
            try {
                if (!namesystem.isFinishedRecover()){
                    System.out.println("当前还没完成元数据恢复，不进行editlog同步......");
                    Thread.sleep(1000);
                    continue;
                }
                JSONArray editsLogs = nameNodeRpcClient.fetchEditsLog(namesystem.getFSImage().getMaxTxid());
                if (editsLogs.size() == 0){
                    System.out.println("没有拉取到任何一条editslog，等待1秒后继续拉取");
                    Thread.sleep(3000);
                    continue;
                }
                for(int i = 0; i < editsLogs.size(); i++) {
                    JSONObject editsLog = editsLogs.getJSONObject(i);
                    System.out.println("拉取到一条editslog：" + editsLog.toJSONString());
                    String op = editsLog.getString("OP");

                    if(op.equals("MKDIR")) {
                        String path = editsLog.getString("PATH");
                        try {
                            namesystem.mkdir(editsLog.getLongValue("txid"),path);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
