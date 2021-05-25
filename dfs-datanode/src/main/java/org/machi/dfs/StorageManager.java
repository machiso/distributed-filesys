package org.machi.dfs;

import java.io.File;
import java.util.Arrays;

/**
 * 存储管理组件
 * @author machi
 */
public class StorageManager {

    /**
     * 获取存储信息
     * @return
     */
    public StorageInfo getStorageInfo() {
        StorageInfo storageInfo = new StorageInfo();

        File file = new File(DataNodeConfig.DATA_DIR);
        if (!file.exists()){
            return null;
        }
        File[] children = file.listFiles();
        if (children == null || children.length == 0){
            return null;
        }

        Arrays.stream(children).forEach(child -> scanfile(child,storageInfo));

        return storageInfo;
    }

    //扫描文件
    public void scanfile(File dir,StorageInfo storageInfo) {
        if (dir.isFile()){
            String path = dir.getPath();
            path = path.substring(DataNodeConfig.DATA_DIR.length() - 2);
            // \image\product\iphone.jpg
            path = path.replace("\\\\", "/"); // /image/product/iphone.jpg

            storageInfo.addFilename(path);
            storageInfo.addStoredDataSize(dir.length());

            return;
        }
    }
}
