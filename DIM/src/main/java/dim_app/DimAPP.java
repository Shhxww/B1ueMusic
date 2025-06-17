package dim_app;

import base.BaseApp;
import bean.TableProcessDim;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Connection;
import util.FlinkSourceUtil;
import util.HBaseUtil;

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
                "dim_APP"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, OutputTag<String> Dirty, OutputTag<String> Late) {
//        TODO  1、读取维度层配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("b1uemusic_dim_config", "dim_conf");
        DataStreamSource<String> confDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource").setParallelism(1);

//        TODO  2、转换数据类型
        SingleOutputStreamOperator<TableProcessDim> dimConf = confDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                TableProcessDim result;
                if (op.equals("d")) {
                    result = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    result = jsonObject.getObject("after", TableProcessDim.class);
                }
                result.setOp(op);
                return result;
            }
        });

//        TODO  3、根据配置在HBase创建 /修改表
        dimConf.process(new ProcessFunction<TableProcessDim, TableProcessDim>() {
//            设置HBase连接
            private Connection hbaseconnection;

//            初始化HBaase连接
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseconnection = HBaseUtil.getHBaseConnection();
            }

//            程序结束，释放连接
            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseconnection);
            }

//            向HBase进行维度表的增删改
            @Override
            public void processElement(TableProcessDim dimconf, ProcessFunction<TableProcessDim, TableProcessDim>.Context ctx, Collector<TableProcessDim> out) throws Exception {
//                获取数据操作类型
                String op = dimconf.getOp();
                String sinkFamily = dimconf.getSinkFamily();
                if (op.equals("d")) {
//                    判断，如果数据类型是删除就向HBase删除该维度表
                    HBaseUtil.dropTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable());
                } else if (op.equals("r") || op.equals("c")) {
//                    如果类型为全量读取和创建就向HBase创建该维度表
                    HBaseUtil.createTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable(), sinkFamily);
                } else if (op.equals("u")) {
//                    如果类型为更改就向HBase删除并重新创建该维度表
                    HBaseUtil.dropTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable());
                    HBaseUtil.createTable(hbaseconnection, "B1ueMusic_DIM", dimconf.getSinkTable(), sinkFamily);
                }
                out.collect(dimconf);
            }
        });

//        TODO  4、将数据流进行广播
        MapStateDescriptor<String,TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("dimConf", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> dimbroadcastDS = dimConf.broadcast(mapStateDescriptor);

//        TODO  5、读取业务数据，并转换为jsonobj类型
        DataStreamSource<String> dimDS = env.fromSource(FlinkSourceUtil.getMySqlSource("b1uemusic"), WatermarkStrategy.noWatermarks(), "dimDS");
        SingleOutputStreamOperator<JSONObject> dimjsonDS = dimDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });

 //        TODO  6、联合维度配置广播流，过滤出维度表数据，并向HBase维度表插入/删除（同时清空redis缓存）
        dimjsonDS.connect(dimbroadcastDS).process(new BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>() {
//            设置HBase连接
            Connection hbaseconnection;
//            初始化连接
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseconnection = HBaseUtil.getHBaseConnection();
            }
//            程序结束释放连接
            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseconnection);
            }

            /**
             * 对业务数据进行处理
             * @param jsonObj 业务数据
             * @param ctx 上下文（主要用来获取广播状态）
             * @param out 数据接收器
             * @throws Exception
             */
            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//                获取广播状态：
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//                获取输入表
                String sourceTable = jsonObj.getJSONObject("source").getString("table");
//                过滤出维度表数据
                if (broadcastState.contains(sourceTable)) {
//                    获取数据操作类型
                    String op = jsonObj.getString("op");
                    TableProcessDim tableProcessDim = broadcastState.get(sourceTable);
                    if (op.equals("d")) {
//                        判断数据为删除操作，向HBase删除该数据
                        HBaseUtil.deleteRow(
                                hbaseconnection,
                                "B1ueMusic_DIM",
                                tableProcessDim.getSinkTable(),
                                tableProcessDim.getSinkRowKey()
                        );
//                        删除Redis中的缓存
                    }else {
                        if (op.equals("u")) {
//                            删除Redis中的缓存

                        }
//                        获取插入表数据
                        JSONObject data = jsonObj.getJSONObject("after");
                        String rowkey = tableProcessDim.getSinkRowKey()+data.getLong(tableProcessDim.getSinkRowKey()).toString();
//                        向HBase插入数据
                        HBaseUtil.putRow(
                                hbaseconnection,
                                "B1ueMusic_DIM",
                                tableProcessDim.getSinkTable(),
                                rowkey,
                                tableProcessDim.getSinkFamily(),
                                data
                        );
                    }
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                将维度配置表的数据更新至广播状态中
                ctx.getBroadcastState(mapStateDescriptor).put(value.getSourceTable(),value);
            }
        });

//        TODO  7、执行程序
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}