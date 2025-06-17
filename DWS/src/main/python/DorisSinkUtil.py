"""
@基本功能: 
@program: python
@author: B1ue
@createTime: 2025/6/15 下午11:37
"""


def DorisSinkUtil(spark,DataBase_TableName,Table_Columns):

    # 配置Doris连接参数
    doris_options = {
        f"doris.fenodes": "node1:7030",
        "doris.table.identifier": {DataBase_TableName},
        "doris.user": "root",
        "doris.password": "000000",
        "doris.write.fields": {Table_Columns}
    }

    # 写入Doris
    spark\
        .sql(f"select {Table_Columns} from {DataBase_TableName}")\
        .write.format("doris") \
        .options(**doris_options) \
        .mode("append") \
        .save()
