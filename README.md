
## 1.添加Spartan引擎 

### 添加spartan_data以及spartan_index

在storage/spartan目录创建

```
spartan_data.cc
spartan_data.h
spartan_index.cc
spartan_index.h
```

### 编译修改
- 拷贝example目录修改为spartan,并将含example关键字修改为spartan
- 修改CMakeLists.txt,添加spartan_data.cc
- 将spartan_data.h添加到ha_spartan.h中
- 重新make && make install

### 将spartan添加到server中

#### 使用插件方式
```
INSTALL PLUGIN spartan SONAME 'ha_spartan.so';

mysql> show plugins;
+------------+--------+----------------+---------------+---------+
| Name       | Status | Type           | Library       | License |
+------------+--------+----------------+---------------+---------+
| binlog     | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| ARCHIVE    | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| BLACKHOLE  | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| CSV        | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| MEMORY     | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| InnoDB     | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| MyISAM     | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| MRG_MYISAM | ACTIVE | STORAGE ENGINE | NULL          | GPL     |
| EXAMPLE    | ACTIVE | STORAGE ENGINE | ha_example.so | GPL     |
| SPARTAN    | ACTIVE | STORAGE ENGINE | ha_spartan.so | GPL     |
+------------+--------+----------------+---------------+---------+
10 rows in set (0.00 sec)
```

#### 将spartan引擎添加到server中

修改my_config.h,添加如下内容:
 
```c
/*BEGIN GUOSONG MODIFICATION*/
#define WITH_SPARTAN_STORAGE_ENGINE 1
/*END GUOSONG MODIFICATION*/

```

修改handler.h,添加如下内容:

```c
enum legacy_db_type
{
    ...
  DB_TYPE_SPARTAN_DB,
  DB_TYPE_FIRST_DYNAMIC=42,
  DB_TYPE_DEFAULT=127 // Must be last
    ...
}
```

## 2.创建Spartan引擎表

### 2.1 更新ha_spartan.h头文件

```c
#include "spartan_data.h"

class Spartan_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Spartan_share();
  ~Spartan_share()
  {
    /*BEGIN GUOSONG MODIFICATION*/
    if(data_class != NULL)
      delete data_class;
    data_class = NULL;
    /*END GUOSONG MODIFICATION*/
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
  /*BEGIN GUOSONG MODIFICATION*/
  Spartan_data *data_class;
  /*END GUOSONG MODIFICATION*/ 
};
```

### 2.2 更新ha_spartan.cc类文件

```c
Spartan_share::Spartan_share()
{
  thr_lock_init(&lock);       
  mysql_mutex_init(ex_key_mutex_Spartan_share_mutex,
    ¦   ¦   ¦   ¦  &mutex, MY_MUTEX_INIT_FAST);
  /*BEGIN GUOSONG MODIFICATION*/ 
  data_class = new Spartan_data();
  /*END GUOSONG MODIFICATION*/
}
```

### 2.3 更新文件后缀扩展

```c
/*BEGIN GUOSONG MODIFICATION*/
#define SDE_EXT ".sde"
#define SDI_EXT ".sdi"
/*END GUOSONG MODIFICATION*/

static const char *ha_spartan_exts[] = {
/*BEGIN GUOSONG MODIFICATION*/
  SDE_EXT,
  SDI_EXT,
/*END GUOSONG MODIFICATION*/

  NullS
};
```

### 2.4 修改ha_spartan.cc中create函数

```c
int ha_spartan::create(const char *name, TABLE *table_arg,
    ¦   ¦   ¦   ¦   ¦  HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_spartan::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  /*BEGIN GUOSONG MODIFICATION*/
  char name_buff[FN_REFLEN];

  if(!(share = get_share()))
    ¦ DBUG_RETURN(1);

  if(share->data_class->create_table(fn_format(name_buff, name, "",SDE_EXT,
    ¦   ¦   ¦   ¦ MY_REPLACE_EXT|MY_UNPACK_FILENAME)))
    ¦   DBUG_RETURN(-1);
  share->data_class->close_table();
  /*END GUOSONG MODIFICATION*/
  DBUG_RETURN(0);
}
```

### 2.5 修改ha_spartan.cc中open函数

```c
int ha_spartan::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_spartan::open");
  /*BEGIN GUOSONG MODIFICATION*/
  Spartan_share *share;
  char name_buff[FN_REFLEN];
  if (!(share = get_share()))
    DBUG_RETURN(1);
  share->data_class->open_table(fn_format(name_buff, name, "", SDE_EXT,
              MY_REPLACE_EXT|MY_UNPACK_FILENAME));

  /*if (!(share = get_share()))
    DBUG_RETURN(1);*/
  /*END GUOSONG MODIFICATION*/

  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}

```

### 2.6 修改ha_spartan.cc中delete_table函数

```c
int ha_spartan::delete_table(const char *name)
{
  DBUG_ENTER("ha_spartan::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  /*BEGIN GUOSONG MODIFICATION*/
  char name_buff[FN_REFLEN];
  my_delete(fn_format(name_buff, name, "",
          SDE_EXT,MY_REPLACE_EXT|MY_UNPACK_FILENAME), MYF(0));
  /*END GUOSONG MODIFICATION*/
  DBUG_RETURN(0);
}
```

### 2.7 修改ha_spartan.cc中rename_table

```c
int ha_spartan::rename_table(const char * from, const char * to)
{
  /*BEGIN GUOSONG MODIFICATION*/
  char data_from[FN_REFLEN];
  char data_to[FN_REFLEN];
  int i = 0;

  DBUG_ENTER("ha_spartan::rename_table ");
  /*i = my_copy(fn_format(data_from, from, "", SDE_EXT,
          MY_REPLACE_EXT|MY_UNPACK_FILENAME),
          fn_format(data_to, to,"",SDE_EXT,
          MY_REPLACE_EXT|MY_UNPACK_FILENAME), MYF(0));*/
  my_delete(data_from, MYF(0));
  DBUG_RETURN(i);
  /*DBUG_RETURN(HA_ERR_WRONG_COMMAND);*/
}
```

### 2.8 测试

```sql
mysql> show create table t1\G
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `a` int(11) DEFAULT NULL
) ENGINE=spartan DEFAULT CHARSET=utf8

mysql> system ls -lh ./data/test/t1.*
-rw-rw---- 1 my5627 mysql  8.4K May  8 16:37 ./data/test/t1.frm
-rw-rw---- 1 my5627 my5627    0 May  8 17:10 ./data/test/t1.sde
```

## 3 支持insert操作

### 3.0 insert操作流程

![insert](images/insert.png)

### 3.1 增加data_file对应的pos变量

```c
class ha_spartan: public handler
{
  THR_LOCK_DATA lock;      ///< MySQL lock
  Spartan_share *share;    ///< Shared lock info
  Spartan_share *get_share(); ///< Get the share
/*BEGIN GUOSONG MODIFICATION*/
/*data文件scan的时候文件fd当前位置pos*/
  off_t current_position;
/*END GUOSONG MODIFICATION*/
...
}
```

### 3.2 在ha_spartan.cc中修改rnd_init函数

```c
int ha_spartan::rnd_init(bool scan)
{
  DBUG_ENTER("ha_spartan::rnd_init");
  /*START GUOSONG MODIFICATION*/
  current_position = 0;
  stats.records = 0;
  ref_length = sizeof(long long);
  /*END GUOSONG MODIFICATION*/
  DBUG_RETURN(0);
}
```
进行table scan的时候一定出现设置.

### 3.3 在ha_spartan.cc中修改rnd_next函数

遍历每个行的时候会访问.
从数据文件中获取每一行数据,并检测是否到文件尾部.


```c
int ha_spartan::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_spartan::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  /*BEGIN GUOSONG MODIFICATION*/
  rc = share->data_class->read_row(buf, table->s->rec_buff_length, 
                                    current_position);
  if (rc != -1)
    current_position = (off_t)share->data_class->cur_position();
  else
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  stats.records++;
  /*END GUOSONG MODIFICATION*/
  /*rc= HA_ERR_END_OF_FILE;*/
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}
```

### 3.4更新position位置信息

```c

void ha_spartan::position(const uchar *record)
{
  DBUG_ENTER("ha_spartan::position");
  /*BEGIN GUOSONG MODIFICATION*/
  my_store_ptr(ref, ref_length, current_position);
  /*END GUOSONG MODIFICATION*/
  DBUG_VOID_RETURN;
}
```

### 3.5 保存当前pos,并读取其下一行

```c
int ha_spartan::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_spartan::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  current_position = (off_t)my_get_ptr(pos, ref_length);
  rc = share->data_class->read_row(buf, current_position, -1);
  /*rc= HA_ERR_WRONG_COMMAND;*/
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}
```

### 3.6 修改ha_spartan.cc中的info函数

```c
int ha_spartan::info(uint flag)
{
  DBUG_ENTER("ha_spartan::info");
  if(stats.records<2)
    stats.records = 2;
  DBUG_RETURN(0);
}
```

### 3.7 修改ha_spartan::write_row实现真正文件的操作

```c
int ha_spartan::write_row(uchar *buf)
{
  DBUG_ENTER("ha_spartan::write_row");
  /*
    spartan of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  long long pos;
  ha_statistic_increment(&SSV::ha_write_count);
  mysql_mutex_lock(&share->mutex);
  pos = share->data_class->write_row(buf, table->s->rec_buff_length);
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}
```

### 3.8 测试

```sql
mysql> show create table t\G
*************************** 1. row ***************************
       Table: t
Create Table: CREATE TABLE `t` (
  `col_a` int(11) DEFAULT NULL,
  `col_b` varchar(10) NOT NULL DEFAULT ' '
) ENGINE=spartan DEFAULT CHARSET=utf8
1 row in set (0.00 sec)

mysql> insert into t values(1, "test1");
Query OK, 1 row affected (0.02 sec)

mysql> insert into t values(1, "test2");
Query OK, 1 row affected (0.00 sec)

mysql> insert into t values(2, "test2"); 
Query OK, 1 row affected (0.01 sec)

mysql> insert into t values(4, "test4");
Query OK, 1 row affected (0.01 sec)

mysql> select * from t;
+-------+-------+
| col_a | col_b |
+-------+-------+
|     1 | test1 |
|     1 | test2 |
|     2 | test2 |
|     4 | test4 |
+-------+-------+
4 rows in set (0.00 sec)
```

### 3.9 增加update|delete|truncate操作

#### update

```c
int ha_spartan::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_spartan::update_row");
  mysql_mutex_lock(&share->mutex);
  share->data_class->update_row((uchar*)old_data, new_data,
        table->s->rec_buff_length, current_position - 
        share->data_class->row_size(table->s->rec_buff_length));
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}
```

#### delete

```c
int ha_spartan::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_spartan::delete_row");
  mysql_mutex_lock(&share->mutex);
  share->data_class->delete_row((uchar*)buf,
          table->s->rec_buff_length,
          current_position - 
          share->data_class->row_size(table->s->rec_buff_length));
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}
```

#### delete_all_rows

```c
int ha_spartan::delete_all_rows()
{
  DBUG_ENTER("ha_spartan::delete_all_rows");
  mysql_mutex_lock(&share->mutex);
  share->data_class->trunc_table();
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}
```

#### truncate

```c
int ha_spartan::truncate()
{
  DBUG_ENTER("ha_spartan::truncate");
  mysql_mutex_lock(&share->mutex);
  share->data_class->trunc_table();
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}
```

## 4.增加索引功能

### 4.1 增加索引的创建 

#### 4.1.1 修改storage/spartan/CMakeLists.txt

```c
SET(SPARTAN_PLUGIN_STATIC "spartan")
SET(SPARTAN_PLUGIN_DYNAMIC "spartan")
SET(SPARTAN_SOURCES ha_spartan.cc spartan_data.cc spartan_index.cc)
MYSQL_ADD_PLUGIN(spartan ${SPARTAN_SOURCES} STORAGE_ENGINE MODULE_ONLY)
TARGET_LINK_LIBRARIES(spartan mysys)
```

之前出现过生成的ha_spartan.so找不到my_copy的问题,可以通过最后一行解决,更多参考[cmake 添加头文件目录，链接动态、静态库](https://www.cnblogs.com/binbinjx/p/5626916.html)

#### 4.1.2 修改头文件ha_spartan.h

```c
#include "spartan_index.h"

class Spartan_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Spartan_share();
  ~Spartan_share()
  {
    /*BEGIN GUOSONG MODIFICATION*/
    if(data_class != NULL)
      delete data_class;
    if(index_class != NULL)
      delete index_class;
    data_class = NULL;
    index_class = NULL;

    /*END GUOSONG MODIFICATION*/
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
  /*BEGIN GUOSONG MODIFICATION*/
  Spartan_data *data_class;
  Spartan_index *index_class;
  /*END GUOSONG MODIFICATION*/ 
};
```

#### 4.1.3 修改ha__spartan.h增加索引

```c
class ha_spartan: public handler
{
  THR_LOCK_DATA lock;      ///< MySQL lock
  Spartan_share *share;    ///< Shared lock info
  Spartan_share *get_share(); ///< Get the share
/*BEGIN GUOSONG MODIFICATION*/
/*data文件scan的时候文件fd当前位置pos*/
  off_t current_position;
/*END GUOSONG MODIFICATION*/

public:
  ha_spartan(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_spartan()
  {
  }

  const char *index_type(uint inx) { return "Spartan_index"; }

  ulonglong table_flags() const
  {
    return HA_NO_BLOBS | HA_NO_AUTO_INCREMENT| HA_BINLOG_STMT_CAPABLE;
  }

  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return (HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE|
            HA_READ_ORDER | HA_KEYREAD_ONLY);
  }

  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

  uint max_supported_keys()          const { return 1; }
  uint max_supported_key_parts()     const { return 1; }
  uint max_supported_key_length()    const { return 128; }
```

#### 4.1.3测试

```sql
mysql> show create table t_idx\G
*************************** 1. row ***************************
       Table: t_idx
Create Table: CREATE TABLE `t_idx` (
  `col_a` int(11) NOT NULL DEFAULT '0',
  `col_b` varchar(10) NOT NULL DEFAULT ' ',
  PRIMARY KEY (`col_a`)
) ENGINE=Spartan DEFAULT CHARSET=utf8
1 row in set (0.00 sec)

mysql> show index from t_idx\G
*************************** 1. row ***************************
        Table: t_idx
   Non_unique: 0
     Key_name: PRIMARY
 Seq_in_index: 1
  Column_name: col_a
    Collation: A
  Cardinality: NULL
     Sub_part: NULL
       Packed: NULL
         Null: 
   Index_type: Spartan_index
      Comment: 
Index_comment: 
1 row in set (0.00 sec)
```

### 4.2 支持索引

#### 4.2.1 修改文件列表
- ha_spartan::open
- ha_spartan::create
- ha_spartan::close
- ha_spartan::get_key
- ha_spartan::get_key_len
- ha_spartan::index_key_map
- ha_spartan::update_row
- ha_spartan::write_row
- ha_spartan::delete_row
- ha_spartan::delete_table
- ha_spartan::rename_table
- ha_spartan::index_next
- ha_spartan::index_prev
- ha_spartan::index_first
- ha_spartan::index_last

具体可以参考文件上述函数的位置.

#### 4.2.2 测试

```sql
mysql> select * from t_idx where col_a >=10;
+-------+--------+
| col_a | col_b  |
+-------+--------+
|    10 | test10 |
|    11 | test11 |
|    12 | test12 |
+-------+--------+

```

```c
Breakpoint 3, ha_spartan::index_next (this=0x7fff340284b0, buf=0x7fff34028700 "\n") at /home/guosong/source/mysql-5.6.27/storage/spartan/ha_spartan.cc:539
539       uchar *key = 0;
(gdb) bt
#0  ha_spartan::index_next (this=0x7fff340284b0, buf=0x7fff34028700 "\n") at /home/guosong/source/mysql-5.6.27/storage/spartan/ha_spartan.cc:539
#1  0x0000000000652497 in handler::ha_index_next (this=0x7fff340284b0, buf=0x7fff34028700 "\n") at /home/guosong/source/mysql-5.6.27/sql/handler.cc:2812
#2  0x000000000065a435 in handler::read_range_next (this=0x7fff340284b0) at /home/guosong/source/mysql-5.6.27/sql/handler.cc:6740
#3  0x000000000065846c in handler::multi_range_read_next (this=0x7fff340284b0, range_info=0x7ffff7f64bb0) at /home/guosong/source/mysql-5.6.27/sql/handler.cc:5843
#4  0x000000000098c648 in QUICK_RANGE_SELECT::get_next (this=0x7fff3402c630) at /home/guosong/source/mysql-5.6.27/sql/opt_range.cc:10644
#5  0x00000000009a638c in rr_quick (info=0x7fff3402e6a8) at /home/guosong/source/mysql-5.6.27/sql/records.cc:369
#6  0x00000000007c2bdd in sub_select (join=0x7fff34005c18, join_tab=0x7fff3402e618, end_of_records=false)
    at /home/guosong/source/mysql-5.6.27/sql/sql_executor.cc:1259
#7  0x00000000007c25c0 in do_select (join=0x7fff34005c18) at /home/guosong/source/mysql-5.6.27/sql/sql_executor.cc:933
#8  0x00000000007c0473 in JOIN::exec (this=0x7fff34005c18) at /home/guosong/source/mysql-5.6.27/sql/sql_executor.cc:194
#9  0x000000000082342c in mysql_execute_select (thd=0x4a76b80, select_lex=0x4a79118, free_join=true) at /home/guosong/source/mysql-5.6.27/sql/sql_select.cc:1100
#10 0x0000000000823743 in mysql_select (thd=0x4a76b80, tables=0x7fff34005288, wild_num=1, fields=..., conds=0x7fff340059b8, order=0x4a792e0, group=0x4a79218, 
    having=0x0, select_options=2147748608, result=0x7fff34005bf0, unit=0x4a78ad0, select_lex=0x4a79118) at /home/guosong/source/mysql-5.6.27/sql/sql_select.cc:1221
#11 0x0000000000821758 in handle_select (thd=0x4a76b80, result=0x7fff34005bf0, setup_tables_done_option=0) at /home/guosong/source/mysql-5.6.27/sql/sql_select.cc:110
#12 0x00000000007fa835 in execute_sqlcom_select (thd=0x4a76b80, all_tables=0x7fff34005288) at /home/guosong/source/mysql-5.6.27/sql/sql_parse.cc:5134
#13 0x00000000007f2f90 in mysql_execute_command (thd=0x4a76b80) at /home/guosong/source/mysql-5.6.27/sql/sql_parse.cc:2656
#14 0x00000000007fd42e in mysql_parse (thd=0x4a76b80, rawbuf=0x7fff34005070 "select * from t_idx where col_a >=10", length=36, parser_state=0x7ffff7f66700)
    at /home/guosong/source/mysql-5.6.27/sql/sql_parse.cc:6386
#15 0x00000000007efdc5 in dispatch_command (command=COM_QUERY, thd=0x4a76b80, packet=0x4b531b1 "", packet_length=36)
    at /home/guosong/source/mysql-5.6.27/sql/sql_parse.cc:1340
#16 0x00000000007eed93 in do_command (thd=0x4a76b80) at /home/guosong/source/mysql-5.6.27/sql/sql_parse.cc:1037
#17 0x00000000007b4d4a in do_handle_one_connection (thd_arg=0x4a76b80) at /home/guosong/source/mysql-5.6.27/sql/sql_connect.cc:982
#18 0x00000000007b4825 in handle_one_connection (arg=0x4a76b80) at /home/guosong/source/mysql-5.6.27/sql/sql_connect.cc:898
#19 0x0000000000b2a799 in pfs_spawn_thread (arg=0x4ae5ef0) at /home/guosong/source/mysql-5.6.27/storage/perfschema/pfs.cc:1860
#20 0x00007ffff7bc7df3 in start_thread () from /lib64/libpthread.so.0
#21 0x00007ffff6c9c2cd in clone () from /lib64/libc.so.6
```
