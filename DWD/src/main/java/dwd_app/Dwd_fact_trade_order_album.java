package dwd_app;

import base.BaseApp;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSQLUtil;
import util.FlinkSourceUtil;

/**
 * @基本功能:   交易域——专辑购买事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-19 21:17:34
 **/

public class Dwd_fact_trade_order_album extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dwd_fact_trade_order_album().start(
                10021,
                4,
                "Dwd_fact_trade_order_album"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、从MySQL读取专辑购买业务数据，转化为JsonObject
         SingleOutputStreamOperator<JSONObject> albumDS = env
                .fromSource(
                        FlinkSourceUtil.getMySqlSource("b1uemusic", "ods_album_order"),
                        WatermarkStrategy.noWatermarks(),
                        "albumDS")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject data = JSONObject.parseObject(value).getJSONObject("after");
                            out.collect(data);
                        } catch (Exception e) {
                            System.out.println("1：转化失败");
                        }
                    }
                });

//        TODO  2、筛选出成功的订单业务数据
        OutputTag<String> collectDirty = new OutputTag<String>("collect_dirty"){};

        SingleOutputStreamOperator<JSONObject> process = albumDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject data, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    Long userId = data.getLong("user_id");
                    Long orderId = data.getLong("order_id");
                    Long orderNum = data.getLong("order_num");
                    Long amount = data.getLong("amount");
                    String statusTypeId = data.getString("status_type_id");
                    if (userId > 0L && orderId > 0L && orderNum > 0L && amount > 0L && statusTypeId != null ) {
                        if (statusTypeId.equals("101")) out.collect(data);
                    } else {
                        data.put("dirty_type", "1");
                        ctx.output(collectDirty, data.toJSONString());
                    }
                } catch (Exception e) {
                    data.put("dirty_type", "2");
                    ctx.output(collectDirty, data.toJSONString());
                }
            }
        });

//        TODO  3、进行维度关联


//        TODO  4、


        env.execute("Dwd_fact_trade_order_album");
    }
}
