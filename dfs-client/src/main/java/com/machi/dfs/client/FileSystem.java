package com.machi.dfs.client;

/**
 * @author machi
 */
public interface FileSystem {

    //创建目录
    void mkdir(String path);

    //优雅关闭
    void shutdown();

    //文件上传
    void upload(byte[] file,String fileName);
}
