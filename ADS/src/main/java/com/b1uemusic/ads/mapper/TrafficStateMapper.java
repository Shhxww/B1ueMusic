package com.b1uemusic.ads.mapper;

import com.b1uemusic.ads.bean.TrafficSongPlayRank;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @基本功能:   流量域 —— 当前歌曲播放人数前十排行
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-21 16:13:07
 **/

public interface TrafficStateMapper {

    @Select("select song_name,current_online_count from B1ueMusic.Dws_Traffic_SongPlay_OnlineNum partition p#{date} order by current_online_count desc limit 10 ")
    List<TrafficSongPlayRank> selectSongPlayRank(Integer date);
}
