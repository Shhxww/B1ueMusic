package dwd_app;

import base.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:   互动域——歌曲评论事实表
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 22:24:00
 **/

public class Dwd_fact_interaction_comment extends BaseApp {

    public static void main(String[] args) {
        new Dwd_fact_interaction_comment().start(
                10018,
                4,
                "Dwd_fact_interaction_comment"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {

    }
}
