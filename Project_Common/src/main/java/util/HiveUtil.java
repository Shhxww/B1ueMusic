package util;

import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-06-05 16:29:48
 **/

public class HiveUtil {
    public static String hiveConfDir = "D:/Java/B1ueMusic/hive-conf";

    public static String hadoopConfDir = "D:/Java/B1ueMusic/hadoop-conf";

    public static HiveCatalog hiveCatalog = new HiveCatalog(
                "myHive",
                "b1uemusic",
                hiveConfDir,
                hadoopConfDir,
                "3.1.3"
        );
}
