package com.b1uemusic.ads.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-21 17:05:07
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficSongPlayRank {
    private String song_name;
    private String current_online_count;
}
