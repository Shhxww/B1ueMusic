package com.b1uemusic.ads.service;

import com.b1uemusic.ads.bean.TrafficSongPlayRank;

import java.util.List;
import java.util.Map;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-21 16:28:21
 **/

public interface TrafficSongPlayRankService {

    List<TrafficSongPlayRank> getSongPlayRank(Integer date);
}
