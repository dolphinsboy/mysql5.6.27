
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

###2.1 更新ha_spartan.h头文件

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

###2.2 更新ha_spartan.cc类文件

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

###2.3 更新文件后缀扩展

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

###2.4 修改ha_spartan.cc中create函数

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

###2.5 修改ha_spartan.cc中open函数

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

###2.6 修改ha_spartan.cc中delete_table函数

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

###2.7 修改ha_spartan.cc中rename_table

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

###2.8 测试

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

