package com.b1uemusic.ads.service.impl;

import com.b1uemusic.ads.bean.TrafficSongPlayRank;
import com.b1uemusic.ads.mapper.TrafficStateMapper;
import com.b1uemusic.ads.service.TrafficSongPlayRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class TrafficSongPlayRankServiceImpl implements TrafficSongPlayRankService {

    @Autowired
    private TrafficStateMapper trafficStateMapper;

    @Override
    public List<TrafficSongPlayRank> getSongPlayRank(Integer date) {
        return trafficStateMapper.selectSongPlayRank(date);
    }
}
