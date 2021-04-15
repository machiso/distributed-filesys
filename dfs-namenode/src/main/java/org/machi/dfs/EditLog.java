package org.machi.dfs;

import com.alibaba.fastjson.JSONObject;

/**
 * @author machi
 * editlog 实体类
 */
public class EditLog {

    //editlog唯一标示，递增趋势，全局唯一
    public long txid;

    //editlog内容
    public String content;

    public EditLog(long txid, String content) {
        this.txid = txid;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid",txid);
        this.content = jsonObject.toJSONString();
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}

