package bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @基本功能:   歌曲收藏表数据类型
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-17 15:57:01
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Dwd_collect {
    private Long collect_id;
    private Long user_id;
    private Long song_id;
    private String channel;
    private Long c_ts;
}
