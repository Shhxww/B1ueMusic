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

    @Select("select songName,num from ddsds partition #{date} ")
    List<TrafficSongPlayRank> selectSongPlayRank(Integer date);
}
