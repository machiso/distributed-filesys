package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @author machi
 */
public class EditsLogFetcher extends Thread{

    private BackupNode backupNode;

    private FSNamesystem namesystem;

    private NameNodeRpcClient namenode;


    public EditsLogFetcher(BackupNode backupNode, FSNamesystem fsNamesystem) {
        this.backupNode = backupNode;
        this.namesystem = fsNamesystem;
        this.namenode = new NameNodeRpcClient();
    }

    @Override
    public void run() {
        while (backupNode.isRunning()){
            JSONArray editsLogs = namenode.fetchEditsLog();
            if (editsLogs.size() == 0){
                System.out.println("没有拉取到任何一条editslog，等待1秒后继续拉取");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
        }
    }
}
