package dim_app;

import base.BaseApp;
import base.DWSBaseApp;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.debezium.data.Json;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSourceUtil;

/**
 * @基本功能:   公共维度层实时同步和数据导入
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-13 16:05:48
 **/

public class DimAPP extends BaseApp {
    public static void main(String[] args) throws Exception {
//        启动程序
        new DimAPP().start(
                                10011,
                                4,
                                "dim_APP");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  1、读取维度层配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("B1ueMusic_dimconf", "dimconf");
        DataStreamSource<String> confDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
//        TODO  2、转换数据类型
        SingleOutputStreamOperator<JSONObject> Conf_Jsonobj = confDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                return JSONObject.parseObject(jsonStr);
            }
        });
//        TODO  3、根据配置在HBase创建 /修改表

//        TODO  4、将数据流进行广播
//        TODO  5、读取业务数据
//        TODO  6、联合广播流，过滤出维度表数据
//        TODO  7、写入HBase维度表
//        TODO  8、

    }
}
