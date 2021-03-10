package com.machi.dfs.client;

import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.machi.rpc.model.MkdirRequest;
import org.machi.rpc.model.MkdirResponse;
import org.machi.rpc.service.NameNodeServiceGrpc;

/**
 * @author machi
 */
public class FileSystemImpl implements FileSystem{

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void mkdir(String path) {

        MkdirRequest mkdirRequest = MkdirRequest.newBuilder()
                .setPath(path)
                .build();

        MkdirResponse mkdirResponse = namenode.mkdir(mkdirRequest);
        System.out.println("收到namenode返回的注册响应："+mkdirResponse.getStatus());
    }
}
