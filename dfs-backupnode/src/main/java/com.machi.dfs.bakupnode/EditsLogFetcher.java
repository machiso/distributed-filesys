package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Optional;
import java.util.stream.Stream;

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

            for(int i = 0; i < editsLogs.size(); i++) {
                JSONObject editsLog = editsLogs.getJSONObject(i);
                String op = editsLog.getString("OP");

                if(op.equals("MKDIR")) {
                    String path = editsLog.getString("PATH");
                    try {
                        namesystem.mkdir(path);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
