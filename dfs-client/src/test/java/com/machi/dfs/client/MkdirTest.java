package com.machi.dfs.client;

/**
 * @author machi
 */
public class MkdirTest {

    public static void main(String[] args) {

        final FileSystemImpl filesystem = new FileSystemImpl();

        for(int j = 0; j < 40; j++) {
            new Thread() {
                public void run() {
                    for(int i = 0; i < 50; i++) {
                        try {
                            filesystem.mkdir("/usr/warehouse/hive" + i + "_" + Thread.currentThread().getName());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

            }.start();
        }
    }
}
