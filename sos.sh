#!/usr/bin/env bash
#紧急救援，故障收集
#根据自己的情况调整用户名 密码 和路径
#不做任何支持和维护
mysql_port="3309"
ip="192.168.80.129"
time="2018"
file=${ip}_${mysql_port}_${time}

mkdir -p /data/mysql_pack/log/$file/log

for((i=1;i<=2;i++));
do
    pt-mysql-summary  --user=admin  --password= --host=127.0.0.1 --port=$mysql_port >> /data/mysql_pack/log/$file/mysql-summary.log
    pt-summary >> /data/mysql_pack/log/$file/pt-summary .log
done

top -b -n 10 -d 3 > /data/mysql_pack/log/$file/top.log
free -m >  /data/mysql_pack/log/$file/free.log
free -g >> /data/mysql_pack/log/$file/free.log
df -h > /data/mysql_pack/log/$file/df.log
df -i >> /data/mysql_pack/log/$file/df.log
vmstat -a 2 5 > /data/mysql_pack/log/$file/vmstat.log
iostat -x 10 5  > /data/mysql_pack/log/$file/iostat.log
iotop -b --iter=10 > /data/mysql_pack/log/$file/iotop.log
/usr/bin/dmesg > /data/mysql_pack/log/$file/dmesg.log
cp -rf  /var/log/messages*  /data/mysql_pack/log/$file/log/
cat /proc/meminfo > /data/mysql_pack/log/$file/meminfo.log
cat /proc/cpuinfo  > /data/mysql_pack/log/$file/cpuinfo.log
mysql_pid=`cat /data/mysql/3309/data/mysql.pid`
lsof -p $mysql_pid > /data/mysql_pack/log/$file/lsof.log
pstack $mysql_pid > /data/mysql_pack/log/$file/pstack.log
/usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show  GLOBAL VARIABLES \G" >> /data/mysql_pack/log/$file/variables.log

for((i=1;i<=10;i++));
do
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "SHOW FULL PROCESSLIST" >> /data/mysql_pack/log/$file/processlist.log
    sleep 1
done

for((i=1;i<=3;i++));
do
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show GLOBAL status\G " >> /data/mysql_pack/log/$file/status.log
    sleep 5

done

/usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show slave status\G" >> /data/mysql_pack/log/$file/slave.log


for((i=1;i<=3;i++));
do
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "select * from sys.schema_table_lock_waits\G " >> /data/mysql_pack/log/$file/locks.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "select * from sys.io_global_by_file_by_bytes\G " >> /data/mysql_pack/log/$file/IO_global_latency.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "select * from sys.io_global_by_file_by_latency\G " >> /data/mysql_pack/log/$file/IO_File_latency.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "select * from sys.memory_by_thread_by_current_bytes\G " >> /data/mysql_pack/log/$file/Mem_Thread_Current.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "select * from sys.memory_global_by_current_bytes\G " >> /data/mysql_pack/log/$file/Mem_Global_Current.log
    sleep 1

done
for((i=1;i<=5;i++));
do
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show ENGINE innodb status\G " >> /data/mysql_pack/log/$file/innodb_status.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show ENGINE innodb MUTEX\G " >> /data/mysql_pack/log/$file/innodb_status_MUTEX.log
    sleep 1
done


for((i=1;i<=10;i++));
do
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show GLOBAL status like '%Questions%'\G  " >> /data/mysql_pack/log/$file/pqs.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show GLOBAL status like '%Handler_commit%'\G  " >> /data/mysql_pack/log/$file/commit.log
    /usr/local/mysql/bin/mysql -u root -p -S /data/mysql/$mysql_port/data/mysql.sock -e "show GLOBAL status like '%Handler_rollback%'\G  " >> /data/mysql_pack/log/$file/rollback.log

    sleep 1
done


cp -rf /data/mysql/$mysql_port/data/error.log  /data/mysql_pack/log/$file/log/
cp -rf /data/mysql/$mysql_port/data/slow.log  /data/mysql_pack/log/$file/log/
/usr/bin/dmesg > /data/mysql_pack/log/$file/dmesg.log
tar -zcf /data/mysql_pack/log/$file.tar.gz /data/mysql_pack/log/$file/*
