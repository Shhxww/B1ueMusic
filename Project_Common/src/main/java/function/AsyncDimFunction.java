package function;

import bean.Constant;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.com.google.common.util.concurrent.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import util.HBaseUtil;
import util.RedisUtil;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-21 16:07:38
 **/

public abstract class AsyncDimFunction extends RichAsyncFunction<JSONObject,JSONObject> {

    //    获取Redis异步连接
    private StatefulRedisConnection<String, String> redisAsyncConn;
    //    获取HBase异步连接
    private AsyncConnection hbaseAsyncConn;


//    初始化HBase、Redis的异步连接
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
    }


//    程序结束、释放连接
    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);

    }

    /**
     *  异步调用, 流中每来一个元素, 这个方法执行一次
     * @param input 需要异步处理的元素
     * @param resultFuture 元素被异步处理完之后, 放入到ResultFuture中,则会输出到后面的流中
     * @throws Exception
     */
    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {

        String rowKey = getRowKey(input);

        String tableName = getTableName();
//        创建异步编排对象
        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
//                         从 redis 读取维度 . 把读取到的维度返回
                        return RedisUtil.readDimAsync(redisAsyncConn, rowKey);
                    }
                }).thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dim) {
//                        若dim为null，则表示在 Redis 没有读到维度,  要从 HBase 读取维度数据
                        if (dim == null){
//                            尝试从HBase中读取维度数据
                            dim = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBaseNameSpace, tableName, rowKey);
//                            若dim不为null，则表示在 HBase 读到
                            if (dim != null) {
//                                将该维度数据加入缓存
                                RedisUtil.writeDimAsync(redisAsyncConn, rowKey, dim, getRedisTTL());
                                System.out.println("未在redis查询到，在 hbase中命中 ");
                            } else {
                                System.out.println("没有在HBase查询到该维度信息");
                            }
                        }else {
                            System.out.println("在Redis中命中");
                        }
//                        返回维度数据
                        return dim;
                    }
                }).thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dim) {
//                        进行维度关联
                        addDims(input, dim);
//                        把维度关联完的数据进行返回
                        resultFuture.complete(Collections.singleton(input));
                    }
                });
    }

    /**
     * 获取 rowkey
     * @param input
     * @return rowkey
     */
    public abstract String getRowKey(JSONObject input);

    /**
     * 获取维度表名
     * @return 维度表名
     */
    public  abstract String getTableName();


    public abstract Long getRedisTTL();

    /**
     * 具体维度关联操作
     * @param input 输入数据
     * @param dim   维度数据
     */
    public  abstract void addDims(JSONObject input, JSONObject dim);
}
