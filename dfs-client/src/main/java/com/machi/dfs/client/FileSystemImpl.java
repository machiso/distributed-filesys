package com.machi.dfs.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
    private NIOClient nioClient;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.nioClient = new NIOClient();
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
    public Boolean upload(byte[] file,String filename,long fileSize) {
        //首先判断文件是否已经存在
        if (!createFile(filename)){
            return false;
        }
        String allocateDataNodes = allocateDataNodes(filename, fileSize);

        JSONArray datanodes = JSONArray.parseArray(allocateDataNodes);
        for(int i = 0; i < datanodes.size(); i++) {
            JSONObject datanode = datanodes.getJSONObject(i);
            String hostname = datanode.getString("hostname");
            int nioPort = datanode.getIntValue("nioPort");
            nioClient.sendFile(hostname, nioPort, file, filename, fileSize);
        }

        return true;
    }

    private boolean createFile(String filename) {
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFilename(filename)
                .build();

        CreateFileResponse response = namenode.create(request);
        return response.getStatus() == 1 ? true : false;
    }

    @Override
    public byte[] download(String filename) throws Exception {
        String dataodeInfo = getDownloadDatanode(filename);
        JSONObject datainfo = JSONObject.parseObject(dataodeInfo);
        //向datanode发送请求
        nioClient.readFile(datainfo.getString("hostname"),datainfo.getInteger("port"),datainfo.getString("filename"));
        return null;
    }

    private String getDownloadDatanode(String filename) {
        GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                .setFilename(filename)
                .build();

        GetDataNodeForFileResponse response = namenode.getDataNodeForFile(request);
        return response.getDatanodeInfo();

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
