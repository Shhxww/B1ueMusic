package util;

import com.alibaba.fastjson.JSONObject;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-22 23:13:11
 **/

public class FlinkDirtyDateUtil {

    public static String Type1(JSONObject value){
        JSONObject dirty_data = new JSONObject();
        dirty_data.put("common", value);
        dirty_data.put("dirty_type", "类型缺失或不在逻辑范围内");
        return dirty_data.toJSONString();
    }

    public static String Type2(JSONObject value){
        JSONObject dirty_data = new JSONObject();
        dirty_data.put("common", value);
        dirty_data.put("dirty_type", "缺失具体数据");
        return dirty_data.toJSONString();
    }

}
