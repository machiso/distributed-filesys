package com.machi.dfs.client;

/**
 * @author machi
 */
public class MkdirTest {

    public static void main(String[] args) {

        FileSystemImpl fileSystem = new FileSystemImpl();
        fileSystem.mkdir("/usr/local/git");
        fileSystem.mkdir("/usr/local/spark");
        fileSystem.mkdir("/root/mq/kafka");
    }
}
