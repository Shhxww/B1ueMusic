package function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-25 20:14:59
 **/

public class DimAssFunction {

    /**
     * 关联上一首歌曲维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assLeadSong (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        try{
                            String leadSongId = input.getLong("lead_song_id").toString();
                            return "song_id" + leadSongId;
                        }catch (Exception e){
                            return "";
                        }
                    }

                    @Override
                    public String getTableName() {
                        return "dim_song";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        if (input.getLong("lead_song_id") == null || dim == null) {
                            input.put("lead_song_name", "");
                            input.put("lead_singer_id", "");
                            input.put("lead_song_type_id", "");
                            input.put("lead_song_duration", "");
                        } else {
                            String songName = dim.getString("song_name");
                            String singerId = dim.getString("singer_id");
                            String leadSongTypeId = dim.getString("song_type_id");
                            String songDuration = dim.getString("song_duration");
                            input.put("lead_song_name", songName);
                            input.put("lead_singer_id", singerId);
                            input.put("lead_song_type", leadSongTypeId);
                            input.put("lead_song_duration", songDuration);
                        }
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    /**
     * 关联歌曲维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assSong (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "song_id"+input.getLong("song_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_song";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String songName = dim.getString("song_name");
                        String singerId = dim.getString("singer_id");
                        String type = dim.getString("song_type_id");
                        String songDuration = dim.getString("song_duration");

                        input.put("song_name", songName);
                        input.put("singer_id", singerId);
                        input.put("song_type", type);
                        input.put("song_duration", songDuration);
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    /**
     * 关联上一首歌曲的歌手维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assLeadSinger (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "singer_id" + input.getLong("lead_singer_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_singer";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String singerName = dim.getString("singer_name");
                        String gender = dim.getString("gender");
                        String nationality = dim.getString("nationality");
                        input.put("lead_singer_name", singerName);
                        input.put("lead_singer_gender", gender);
                        input.put("lead_singer_nationality", nationality);
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    /**
     * 关联歌手维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assSinger (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "singer_id" + input.getLong("singer_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_singer";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String singerName = dim.getString("singer_name");
                        String gender = dim.getString("gender");
                        String nationality = dim.getString("nationality");
                        input.put("singer_name", singerName);
                        input.put("singer_gender", gender);
                        input.put("singer_nationality", nationality);
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    /**
     * 关联用户维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assUser (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "user_id" + input.getLong("user_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_user";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String userName = dim.getString("user_name");
                        String gender = dim.getString("gender");
                        input.put("user_name", userName);
                        input.put("user_gender", gender);
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }

    /**
     * 关联省份维度表
     * @param dataStream    数据流
     * @param ttl   Redis缓存的保留时间
     * @return  关联完成的数据流
     */
    public static SingleOutputStreamOperator<JSONObject> assProvince (SingleOutputStreamOperator<JSONObject> dataStream,Long ttl){
        return AsyncDataStream.unorderedWait(
                dataStream,
                new AsyncDimFunction() {
                    @Override
                    public String getRowKey(JSONObject input) {
                        return "province_id" + input.getLong("province_id").toString();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_province";
                    }

                    @Override
                    public Long getRedisTTL() {
                        return ttl;
                    }

                    @Override
                    public void addDims(JSONObject input, JSONObject dim) {
                        String provinceName = dim.getString("province_name");
                        input.put("province_name", provinceName);
                    }
                },
                120,
                TimeUnit.SECONDS
        );
    }


}
