package dwd_app;

import base.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSQLUtil;

/**
 * @基本功能:   流量域——歌曲播放事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-16 23:30:07
 **/

/*
    {"
        common": {
            "play_id": 263336,
            "song_id": 10069,
            "user_id": 10031,
            "complete": 1,
            "start_ts": 1748853375000,
            "end_ts": 1748853440660
            },
        "channel": "Web",
        "mac_id": "924617869754446474",
        "type": "play",
        "ts": 1748853375000}
 */

public class Dwd_fact_traffit_play extends BaseApp {

    public static void main(String[] args) throws Exception {
//        启动程序
        new Dwd_fact_traffit_play()
                .start(
                    10014,
                    4,
                    "Dwd_fact_trade_vip"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
//        TODO  1、创建日志数据映射表
        FlinkSQLUtil.setOds_log(tEnv);

//        TODO  2、过滤出歌曲播放日志数据，并进行清洗
        Table result = tEnv.sqlQuery(
                "select " +
                "cast(common['play_id'] as bigint ) play_id," +
                "cast(common['song_id'] as bigint ) song_id," +
                "cast(common['user_id'] as bigint ) user_id," +
                "cast(common['complete'] as int ) complete," +
                "cast(common['start_ts'] as bigint ) start_ts," +
                "cast(common['end_ts'] as bigint ) end_ts," +
                "channel," +
                "ts " +
                "from Ods_log " +
                "where type = 'play' " +
                "   and cast(common['play_id'] as bigint )>0 " +
                "   and cast(common['user_id'] as bigint )>0" +
                "   and cast(common['song_id'] as bigint )>0 " +
                "   and  cast(common['start_ts'] as bigint ) is not null " +
                "   and cast(common['end_ts'] as bigint ) is not null" +
                "   and cast(common['complete'] as int ) in (0,1) "
        );

//        TODO  3、创建歌曲播放事实表的映射表
         tEnv.executeSql("create table dwd_fact_traffic_play(" +
                " play_id bigint," +
                 "song_id bigint," +
                " user_id bigint," +
                " complete int," +
                 "start_ts bigint," +
                 "end_ts bigint," +
                " channel string," +
                " ts bigint," +
                "primary key(play_id) not enforced" +
                ")"+FlinkSQLUtil.getUpsetKafkaDDLSink("BM_DWD_traffic")
        );

//        TODO  4、将清洗后的数据插入到映射表中去
        result.executeInsert("dwd_fact_traffic_play");
    }

}
