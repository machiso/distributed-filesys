package com.machi.dfs.bakupnode;

import com.alibaba.fastjson.JSONArray;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.zhss.dfs.namenode.rpc.model.FetchEditsLogResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;

/**
 * @author machi
 */
public class NameNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 抓取editslog数据
     * @return
     */
    public JSONArray fetchEditsLog() {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setCode(1)
                .build();

        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        String editsLogJson = response.getEditsLog();

        return JSONArray.parseArray(editsLogJson);
    }


}


