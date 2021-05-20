package com.machi.dfs.client;

import com.zhss.dfs.namenode.rpc.model.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import com.zhss.dfs.namenode.rpc.service.NameNodeServiceGrpc;

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

    @Override
    public void shutdown() {
        ShutdownRequest shutdownRequest = ShutdownRequest.newBuilder()
                .setCode(1)
                .build();
        namenode.shutdown(shutdownRequest);
    }

    @Override
    public void upload(byte[] file,String filename) {
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFilename(filename)
                .build();

        CreateFileResponse response = namenode.create(request);
        int status = response.getStatus();
    }

    //分配双副本对应的数据节点
    private String allocateDataNodes(String filename, long fileSize) {
        AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder()
                .setFilename(filename)
                .setFileSize(fileSize)
                .build();

        AllocateDataNodesResponse response = namenode.allocateDataNodes(request);
        return response.getDatanodes();
    }


}
