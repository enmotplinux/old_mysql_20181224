import pymysql
from DBUtils.PooledDB import PooledDB

import time
import warnings
warnings.filterwarnings("ignore")

def DML_SQL(ipaddress, username, password, db, mysql_port,sql):
    try:
        pool = PooledDB(pymysql,50,host=ipaddress,
                        user=username,passwd=password,db=db,port=int(mysql_port), charset="utf8mb4")
        conn = pool.connection()
        cur=conn.cursor(cursor=pymysql.cursors.DictCursor)
        r=cur.execute(sql)
        r=cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return r
    except pymysql.err.IntegrityError:
        return False
    except pymysql.err.OperationalError:
        return False
    except pymysql.err.ProgrammingError:  # 没有库
        return False
    except pymysql.err.InternalError:  # 没有表
        return False



def MySQL_Status_Run(ipaddress,username,password,db,mysql_port ):

    conn=pymysql.connect(host=ipaddress,user=username,passwd=password,db=db,port=mysql_port)
    cur=conn.cursor()
    cur.execute('SHOW GLOBAL STATUS')
    data_list = cur.fetchall()
    data_dic = {}
    for item in data_list:
        data_dic[item[0]] = item[1]

    time.sleep(1)
    cur.execute('SHOW GLOBAL STATUS')
    data_list_new = cur.fetchall()
    data_dic_new = {}
    cur.close()
    for item_new in data_list_new:
        data_dic_new[item_new[0]] = item_new[1]
    all_read_request=int(data_dic_new['Innodb_buffer_pool_reads']) + int(data_dic_new['Innodb_buffer_pool_read_requests'])
    return{
    'qps':(int(data_dic_new['Questions']) - int(data_dic['Questions'])),
    'tps':(int(data_dic_new['Handler_commit']) + int(data_dic['Handler_rollback']))/1,
    'DML':{"select":(int(data_dic_new['Com_select']) - int(data_dic['Com_select'])),
           'insert':(int(data_dic_new['Com_insert']) - int(data_dic['Com_insert'])),
           'update': (int(data_dic_new['Com_update']) - int(data_dic['Com_update'])),
           'delete': (int(data_dic_new['Com_delete']) - int(data_dic['Com_delete'])),
           "ibp_read":(int(data_dic_new['Innodb_rows_read']) - int(data_dic['Innodb_rows_read'])),
           'ibp_insert':(int(data_dic_new['Innodb_rows_inserted']) - int(data_dic['Innodb_rows_inserted'])),
           'ibp_update': (int(data_dic_new['Innodb_rows_updated']) - int(data_dic['Innodb_rows_updated'])),
           'ibp_delete':(int(data_dic_new['Innodb_rows_deleted']) - int(data_dic['Innodb_rows_deleted'])),
           },
    "Opened":{
        'Open_tables':int(data_dic_new['Open_tables']) ,# 当前打开的表数量
        "Opened_tables": int(data_dic_new['Opened_tables']) , # 已经打开的表的数量，如果Opened_tables较大，table_cache值可能太小。
        'Open_files':int(data_dic_new['Open_files']), # 当前打开的表数量
        'Opened_files':int(data_dic_new['Opened_files']), # 已经打开的表的数量，如果Opened_tables较大，table_cache值可能太小。
        "Opened_table_definitions":int(data_dic_new['Opened_table_definitions']),#已经缓存的.frm文件数量
        "Open_table_definitions":int(data_dic_new['Open_table_definitions']),# 当前缓存的.frm文件数量

    },
   "Threads":{
       "Threads_cached ":int(data_dic_new['Threads_cached']) ,
       'Threads_connected':int(data_dic_new['Threads_connected']) ,
       'Threads_running':int(data_dic_new['Threads_running']) ,
       'Threads_created':int(data_dic_new['Threads_created']) ,
   },
    "tmp":{
         "Created_tmp_disk_tables":int(data_dic_new['Created_tmp_disk_tables']) ,
         "Created_tmp_files":int(data_dic_new['Created_tmp_files']),
         "Created_tmp_tables":int(data_dic_new['Created_tmp_tables']),
    },
     "Handler":{
         "Handler_read_first":int(data_dic_new['Handler_read_first']) ,# 索引中第一条记录被读的次数，如果较高 表明服务器正在执行大量的全索引扫描
         "Handler_read_rnd" : int(data_dic_new['Handler_read_rnd']),  # 根据固定位置读一行的请求数，没有正确使用索引
        "Handler_read_rnd_next":int(data_dic_new['Handler_read_rnd_next']) , # 在数据文件中读下一行的请求数。如果进行大量表扫描，该值较高。没有正确利用索引
     },
    "lock":{
        "Com_lock_tables":int(data_dic_new['Com_lock_tables']),
        "Innodb_row_lock_current_waits":int(data_dic_new['Innodb_row_lock_current_waits']), #当前等等行锁时间
        "Innodb_row_lock_time":int(data_dic_new['Innodb_row_lock_time']),#行锁定花费的总时间 毫秒
        "Innodb_row_lock_time_avg":int(data_dic_new['Innodb_row_lock_time_avg']), #行锁平均锁定时间
        "Innodb_row_lock_time_max": int(data_dic_new['Innodb_row_lock_time_max']), #行锁的最长时间
        "Innodb_row_lock_waits": int(data_dic_new['Innodb_row_lock_waits']), #行锁的次数
        "Table_locks_immediate":int(data_dic_new['Table_locks_immediate']),#立刻获得表锁次数
        "Table_locks_waited":int(data_dic_new['Table_locks_waited']),#表锁
    },
    "binlog":{
        "Binlog_cache_disk_use":int(data_dic_new['Binlog_cache_disk_use']),#使用临时二进制日志换成但超过了binlog_cache_size
        "Binlog_cache_use":int(data_dic_new['Binlog_cache_use']),#使用临时二进制日志缓存的事物数量
        "Binlog_stmt_cache_disk_use":int(data_dic_new['Binlog_stmt_cache_disk_use']), #当非事物语句使用二进制日志缓存，但超binlog_cache_size
        "Binlog_stmt_cache_use":int(data_dic_new['Binlog_stmt_cache_use']) #使用二进制日志缓存文件的非事物语句数量

    },
    "network":{
        "Bytes_sent":(int(data_dic_new['Bytes_sent']) - int(data_dic['Bytes_sent'])), #发送
        "Bytes_received":(int(data_dic_new['Bytes_received']) - int(data_dic['Bytes_received'])), #接收
        "Connections":int(data_dic_new['Connections']), #试图链接MySQL服务器的连接数
        "Aborted_clients":int(data_dic_new['Aborted_clients']), #客户端没有正确关闭连接导致客户端中断
        "Aborted_connects":int(data_dic_new['Aborted_connects']) #失败的连接数

    },
    "innodb_buffer_info":{
        "Innodb_buffer_pool_pages_dirty":int(data_dic_new['Innodb_buffer_pool_pages_dirty']), #当前脏页
        "Innodb_buffer_pool_pages_free":int(data_dic_new['Innodb_buffer_pool_pages_free']),#还没使用到的总数
        "Innodb_buffer_pool_read_requests":int(data_dic_new['Innodb_buffer_pool_read_requests']), #逻辑读
        "Innodb_buffer_pool_reads":int(data_dic_new['Innodb_buffer_pool_reads']),#物理读
        "Innodb_log_waits":int(data_dic_new['Innodb_log_waits']), #必须等待的时间
        "Innodb_log_write_requests":int(data_dic_new['Innodb_log_write_requests']), #日志写请求数
        "Innodb_os_log_pending_writes":int(data_dic_new['Innodb_os_log_pending_writes']),# 值过大，增加 log_buffer_size
        "all_read_request":int(data_dic_new['Innodb_buffer_pool_reads']) +int(data_dic_new['Innodb_buffer_pool_read_requests']),
        "ibp_hint":int(data_dic_new['Innodb_buffer_pool_read_requests']) / all_read_request * 100

    }
}


def MySQL_Index(ipaddress,username,password,db,mysql_port):
    sql = ('select table_schema,table_name,redundant_index_name,sql_drop_index'
                    ' from sys.schema_redundant_indexes limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Data(ipaddress,username,password,db,mysql_port):
    #数据
    sql=('select TABLE_SCHEMA, concat(truncate(sum(data_length)/1024/1024,2),\' MB\') as data_size, ' \
                'concat(truncate(sum(index_length)/1024/1024,2),\'MB\') as index_size from ' \
                'information_schema.tables where TABLE_SCHEMA not in '
                '(\'information_schema\',\'mysql\',\'performance_schema\',\'sys\' ) '
                'group by TABLE_SCHEMA limit 10  ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_ENGINE(ipaddress,username,password,db,mysql_port):
    #存储引擎

    sql =('  SELECT TABLE_SCHEMA,TABLE_NAME,ENGINE '
                'FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN'
                '(\'information_schema\', \'mysql\', \'performance_schema\', \'sys\') AND ENGINE = \'MyISAM\' limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Primary(ipaddress, username, password, db, mysql_port):
    #主键
    sql=('select table_schema,table_name from information_schema.tables  '
                'where (table_schema,table_name) not in'
                '(select distinct table_schema,table_name from '
                'information_schema.columns where COLUMN_KEY=\'PRI\')'
                'and table_schema not in ('
                '\'sys\',\'mysql\',\'information_schema\',\'performance_schema\' )limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def Innodb_Lock(ipaddress, username, password, db, mysql_port):
    #innodb 锁
    sql=('select wait_started,wait_age,locked_table,locked_index,'
         'locked_type,waiting_trx_id,waiting_trx_rows_locked,'
         'waiting_query,blocking_pid,blocking_lock_mode,'
         'blocking_trx_started,blocking_trx_age,sql_kill_blocking_query '
         'from sys.innodb_lock_waits limit 10  ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_trx(ipaddress, username, password, db, mysql_port):
    #事物
    sql=('select trx_id,INNODB_TRX.trx_state,INNODB_TRX.trx_started,se.conn_id '
                'as processlist_id,trx_lock_memory_bytes,se.user,se.command,'
                'se.state,se.current_statement,se.last_statement from '
                'information_schema.INNODB_TRX,sys.session as se where trx_mysql_thread_id=conn_id limit 10  ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Mem(ipaddress, username, password, db, mysql_port):
    #内存消耗
    sql=('select event_name,current_alloc '
                'from sys.memory_global_by_current_bytes limit 10')

    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Temp_Table(ipaddress, username, password, db, mysql_port):
    #show temp
    sql=('select query,db,exec_count,total_latency,memory_tmp_tables,'
                'disk_tmp_tables,tmp_tables_to_disk_pct from '
                'sys.statements_with_temp_tables where db not in (\'sys\',\'mysql\',\'performance_schema\','
                '\'information_schema\',NULL) limit 10  ')

    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def Innodb_Buffer_Table(ipaddress, username, password, db, mysql_port):
#table innodb
    sql=('select * from sys.innodb_buffer_stats_by_table '
                'where object_schema not in'
                ' (\'mysql\',\'InnoDB System\',\'information_schema\','
                '\'performance_schema\',\'sys\') limit 10  ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Table_Statistics(ipaddress, username, password, db, mysql_port):
    #表io消耗
    sql=('select table_schema,table_name,sum(io_read_requests+io_write_requests) io '
                'from sys.schema_table_statistics '
                'where table_schema  not in'
                ' (\'mysql\',\'InnoDB System\',\'information_schema\','
                '\'performance_schema\',\'sys\') group by table_schema,'
                'table_name order by io desc limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_IO_Global(ipaddress, username, password, db, mysql_port):
    sql=('select file,avg_read+avg_write as avg_io'
                ' from sys.io_global_by_file_by_bytes '
                'order by avg_io desc limit 10')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result


def MySQL_Full_Table(ipaddress, username, password, db, mysql_port):
    sql=('select query,db,exec_count,total_latency '
         'from statements_with_full_table_scans   where db not in (\'mysql\','
         '\'InnoDB System\',\'information_schema\',\'performance_schema\',\'sys\') limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)
    return result

def MySQL_Schema_Table_Lock(ipaddress, username, password, db, mysql_port):
    sql=('select object_schema,object_name waiting_thread_id,waiting_pid,'
         'waiting_account,waiting_lock_type,waiting_lock_duration,waiting_query,'
         'sql_kill_blocking_query,sql_kill_blocking_connection '
          'from schema_table_lock_waits where object_schema  not in (\'mysql\','
         '\'InnoDB System\',\'information_schema\',\'performance_schema\',\'sys\') limit 10 ')
    result = DML_SQL(ipaddress, username, password, 'sys', mysql_port, sql)

    return result


def run_inspection(ipaddress,username,password,db,mysql_port):
    mysql_status=MySQL_Status_Run(ipaddress,username,password,db,mysql_port)
    mysql_index=MySQL_Index(ipaddress, username, password, db, mysql_port)
    mysql_data=MySQL_Data(ipaddress, username, password, db, mysql_port)
    mysql_engine=MySQL_ENGINE(ipaddress, username, password, db, mysql_port)
    mysql_primary=MySQL_Primary(ipaddress, username, password, db, mysql_port)
    innodb_lock=Innodb_Lock(ipaddress, username, password, db, mysql_port)
    mysql_mem=MySQL_Mem(ipaddress, username, password, db, mysql_port)
    mysql_trx = MySQL_trx(ipaddress, username, password, db, mysql_port)
    mysql_tmp_table=MySQL_Temp_Table(ipaddress, username, password, db, mysql_port)
    innodb_buffer_table=Innodb_Buffer_Table(ipaddress, username, password, db, mysql_port)
    mysql_table_statistics = MySQL_Table_Statistics(ipaddress, username, password, db, mysql_port)
    mysql_io_global=MySQL_IO_Global(ipaddress, username, password, db, mysql_port)
    mysql_full_table=MySQL_Full_Table(ipaddress, username, password, db, mysql_port)
    mysql_schema_table_lock=MySQL_Schema_Table_Lock(ipaddress, username, password, db, mysql_port)
    mysql_status['mysql_redundant_index']=mysql_index
    mysql_status['mysql_data'] = mysql_data
    mysql_status['mysql_engine'] = mysql_engine
    mysql_status['mysql_primary'] = mysql_primary
    mysql_status['innodb_lock'] = innodb_lock
    mysql_status['mysql_trx'] = mysql_trx
    mysql_status['mysql_mem'] = mysql_mem
    mysql_status['mysql_tmp_table'] = mysql_tmp_table
    mysql_status['innodb_buffer_table'] = innodb_buffer_table
    mysql_status['mysql_table_statistics'] = mysql_table_statistics
    mysql_status['mysql_io_global'] = mysql_io_global
    mysql_status['mysql_full_table'] = mysql_full_table
    mysql_status['schema_table_lock'] = mysql_schema_table_lock
    return mysql_status


#aaa=Run_Inspection('192.168.80.129','admin','redhat','mysql',3309)
#print(aaa)
