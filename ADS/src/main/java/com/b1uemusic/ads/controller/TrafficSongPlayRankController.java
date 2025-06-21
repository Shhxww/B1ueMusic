package com.b1uemusic.ads.controller;

import com.b1uemusic.ads.bean.TrafficSongPlayRank;
import com.b1uemusic.ads.service.TrafficSongPlayRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-21 16:32:07
 **/

@RestController
public class TrafficSongPlayRankController {
    @Autowired
    private TrafficSongPlayRankService trafficSongPlayRankService;

    @RequestMapping("/SongPlayRank")
    public String getSongPlayRank( @RequestParam(value = "date",defaultValue = "0") Integer date) {
        if (date == 0) {

        }

        List<TrafficSongPlayRank> songPlayRank = trafficSongPlayRankService.getSongPlayRank(date);
        return "";
    }

}
