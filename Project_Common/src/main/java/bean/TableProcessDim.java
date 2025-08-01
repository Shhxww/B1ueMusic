package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @基本功能:   维度表配置表数据类型
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-14 23:20:24
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDim {
// 来源表名
    String sourceTable;
    
// 目标表名
    String sinkTable;

// 输出字段
    String sinkColumns;

// 数据到 hbase 的列族
    String sinkFamily;

// sink到 hbase 的时候的主键字段
    String sinkRowKey;

// 配置表操作类型
    String op;
}
