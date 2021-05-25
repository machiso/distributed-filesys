package org.machi.dfs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhss.dfs.namenode.rpc.model.HeartbeatResponse;
/**
 * 心跳组件
 * @author machi
 */
public class HeartBeatManager {

    private NameNodeRpcClient nameNodeRpcClient;

    private StorageManager storageManager;

    public HeartBeatManager(NameNodeRpcClient nameNodeRpcClient,StorageManager storageManager) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.storageManager = storageManager;
    }

    public void start() {
        new HeartbeatThread().start();
    }

    private class HeartbeatThread extends Thread{
        @Override
        public void run() {
            while (true){
                try {
                    HeartbeatResponse heartbeatResponse = nameNodeRpcClient.heartbeat();
                    System.out.println("收到namenode返回的心跳响应"+heartbeatResponse.getStatus());
                    //心跳失败
                    if (heartbeatResponse.getStatus() ==2){
                        JSONArray jsonArray = JSONArray.parseArray(heartbeatResponse.getCommands());
                        for (int i = 0 ; i < jsonArray.size() ; i++){
                            JSONObject command = jsonArray.getJSONObject(i);
                            // 1 - 注册，2 - 全量上报
                            if (command.getInteger("type") == 1){
                                nameNodeRpcClient.register();
                            }else if (command.getInteger("type") == 2){
                                StorageInfo storageInfo = storageManager.getStorageInfo();
                                if (storageInfo != null){
                                    nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
                                }
                            }
                        }
                    }

                    Thread.sleep(30 * 1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
