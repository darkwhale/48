"""
生成hive脚本并运行；
"""

import subprocess
import os


def make_sql(sql_path, *sql_list):

    sql_strs = ';\n'.join([sql for sql in sql_list]) + ';'

    with open(sql_path, 'w') as write_sql:
        write_sql.write(sql_strs)


def hive(dir, database, date):
    # todo 添加sql语言;
    hive_file_path = os.path.join(os.path.dirname(dir), 'hive.sql')

    drop_sql = "drop table if exists src_" + database

    create_sql = "create external table src_" + database + "(insert_day string," \
                                                           " orderno string, " \
                                                           "biztype string, " \
                                                           "transtype string, " \
                                                           "payment string, " \
                                                           "iscod int, " \
                                                           "codcy int, " \
                                                           "codvalue float, " \
                                                           "isinsure int, " \
                                                           "insurevalue float, " \
                                                           "isspu int, " \
                                                           "isinspect int, " \
                                                           "iscustoms int, " \
                                                           "specialbiz string, " \
                                                           "companycode string, " \
                                                           "companyname string, " \
                                                           "gname string, " \
                                                           "gtype string, " \
                                                           "gqty int, " \
                                                           "gwt float, " \
                                                           "gvol float, " \
                                                           "gpkg string, " \
                                                           "gsize float, " \
                                                           "scountry string, " \
                                                           "sprovince string, " \
                                                           "scity string, " \
                                                           "sdistrict string, " \
                                                           "scsrcode string, " \
                                                           "stbid string, " \
                                                           "smobile string, " \
                                                           "smobileattr string, " \
                                                           "smobiletype string, " \
                                                           "stel string, " \
                                                           "sadd string, " \
                                                           "szip string, " \
                                                           "scouriername string, " \
                                                           "scouriermobile string, " \
                                                           "sbrcode string, " \
                                                           "sbrname string, " \
                                                           "sbrtel string, " \
                                                           "sbradd string, " \
                                                           "colltime string, " \
                                                           "ordertime string, " \
                                                           "delytime string, " \
                                                           "eatime string, " \
                                                           "sendtime string, " \
                                                           "signofftime string, " \
                                                           "rcountry string, " \
                                                           "rprovince string, " \
                                                           "rcity string, " \
                                                           "rdistrict string, " \
                                                           "rcsrcode string, " \
                                                           "rtbid string, " \
                                                           "rorgan string, " \
                                                           "rname string, " \
                                                           "rid string, " \
                                                           "rmobile string, " \
                                                           "rmobileattr string, " \
                                                           "rmobiletype string, " \
                                                           "rtel string, " \
                                                           "radd string, " \
                                                           "rzip string, " \
                                                           "rcouriername string, " \
                                                           "rcouriermobile string, " \
                                                           "rbrcode string, " \
                                                           "rbrname string, " \
                                                           "rbrtel string, " \
                                                           "rbradd string, " \
                                                           ")row format delimited " \
                                                           "fields terminated by ',' " \
                                                           "lines terminated by '\\n' " \
                                                           "stored as textfile " \
                                                           "location '/waybill_data/" \
                                                            + database + '/' + date \
                                                            + "'"

    load_data_sql = "load data local inpath '" + dir + "' into table src_" + database

    make_sql(hive_file_path,
             "set hive.exec.compress.output=false",
             "set hive.exec.compress.intermediate=true",
             "set mapred.max.split.size=1000000000",
             "set mapred.min.split.size.per.node=1000000000",
             "set mapred.min.split.size.per.rack=1000000000",
             "set hive.groupby.skewindata=true",
             "set hive.auto.convert.join=true",
             "set hive.exec.dynamic.partition.mode=nostrick",
             "set hive.exec.dynamic.partition=true",
             "set hive.stats.autogather=false",
             "set hive.stats.reliable=false",
             "use waybill",
             drop_sql,
             create_sql,
             load_data_sql
             )

    # todo 待验证；
    # hive_command = "hive -f " + hive_file_path
    #
    # subprocess.run(hive_command,)

