#include "spartan_data.h"
#include <my_dir.h>
#include <string.h>

Spartan_data::Spartan_data(void)
{
    data_file = -1;
    number_records = 0;
    number_del_records = 0;
    /*文件头大小*/
    /*包括如下三个字段:
     * 是否crashed
     * 记录行数
     * 删除的记录行数*/
    header_size = sizeof(bool) + sizeof(int) + sizeof(int);
    /*行记录大小*/
    /*包括如下两个字段:
     * 是否被删除
     * 记录长度*/

    record_header_size = sizeof(uchar) + sizeof(int);
}

Spartan_data::~Spartan_data(void)
{
}

int Spartan_data::create_table(char *path)
{
    DBUG_ENTER("Spartan_data::create_table");
    open_table(path);
    number_records = 0;
    number_del_records = 0;
    crashed = false;
    write_header();
    DBUG_RETURN(0);
}

int Spartan_data::open_table(char *path)
{
    DBUG_ENTER("Spartan_data::open_table");
    data_file = my_open(path,O_RDWR|O_CREAT|O_BINARY|O_SHARE, MYF(0));
    if(data_file == -1)
        DBUG_RETURN(errno);
    DBUG_RETURN(0);
}


int Spartan_data::read_header()
{
    int i;
    int len;
    DBUG_ENTER("Spartan_data::read_header");
    /*初次构建*/
    if(number_records == -1)
    {
        my_seek(data_file, 0l, MY_SEEK_SET, MYF(0));
        /*读取crashed*/
        i = my_read(data_file, (uchar*)&crashed, sizeof(bool), MYF(0));
        /*读取记录行数*/
        i = my_read(data_file,(uchar*)&len, sizeof(int), MYF(0));
        memcpy(&number_records, (uchar*)&len, sizeof(int));
        /*读取被删除的行数*/
        i = my_read(data_file, (uchar*)&len, sizeof(int), MYF(0));
        memcpy(&number_del_records, (uchar*)&len, sizeof(int));
    }else{
        my_seek(data_file, header_size, MY_SEEK_SET, MYF(0));
    }

    DBUG_RETURN(0);
}


int Spartan_data::write_header()
{
    int i;
    DBUG_ENTER("Spartan_data::write_header");
    if(number_records != -1)
    {
        my_seek(data_file, 0l, MY_SEEK_SET, MYF(0));
        i = my_write(data_file, (uchar*)&crashed, sizeof(bool), MYF(0));
        i = my_write(data_file, (uchar*)&number_records, sizeof(int), MYF(0));
        i = my_write(data_file, (uchar*)&number_del_records,sizeof(int), MYF(0));
    }

    DBUG_RETURN(0);
}

long long Spartan_data::write_row(uchar *buf, int length)
{
    long long pos;
    int i;
    int len;
    uchar deleted = 0;

    DBUG_ENTER("Spartan_data::write_row");

    /*定位到文件尾部*/
    pos = my_seek(data_file, 0l, MY_SEEK_END, MYF(0));

    /*在文件末尾追加*/
    /*写入记录头*/
    i = my_write(data_file, &deleted, sizeof(uchar), MYF(0));
    memcpy(&len, &length, sizeof(int));
    i = my_write(data_file, (uchar*)&len, sizeof(int), MYF(0));

    /*写入记录内容*/
    i = my_write(data_file, buf, length, MYF(0));

    if (i == -1)
        pos = i;
    else
    {
        number_records++;
        write_header();
    }
        

    DBUG_RETURN(pos);
}

long long Spartan_data::update_row(uchar *old_rec,uchar *new_rec,
                                int length, long long position)
{
    long long pos;
    long long cur_pos;
    uchar *cmp_rec;
    int len;
    uchar deleted = 0;
    int i = -1;

    DBUG_ENTER("Spartan_data::update_row");

    /*更新首行*/
    if (position == 0)
        position = header_size;
    pos = position;

    if (position == -1)
    {
        /*如果position=-1,逐行查找对应的行*/
        cmp_rec = (uchar*)my_malloc(length, MYF(MY_ZEROFILL|MY_WME));
        pos = 0;
        cur_pos = my_seek(data_file, header_size, MY_SEEK_SET, MYF(0));

        while((cur_pos != -1) && (pos !=-1))
        {
            /*读取一行的内容*/
            pos = read_row(cmp_rec, length, cur_pos);
            /*将与记录前镜像进行比较*/
            if(memcmp(old_rec, cmp_rec, length) == 0)
            {
                /*找到对应的记录*/
                pos = cur_pos;
                cur_pos = -1;
            }else if(pos != -1)
            {
                /*找一行记录进行对比*/
                cur_pos = cur_pos + length + record_header_size;
            }
        }
        my_free(cmp_rec);
    }

    if(pos != -1)
    {
        my_seek(data_file, pos, MY_SEEK_SET, MYF(0));
        i = my_write(data_file, &deleted, sizeof(uchar), MYF(0));
        memcpy(&len, &length, sizeof(int));
        i = my_write(data_file, (uchar*)&len, sizeof(int), MYF(0));
        pos = i;
        i = my_write(data_file, new_rec, length, MYF(0));
    }
    DBUG_RETURN(pos);
}


int Spartan_data::delete_row(uchar *old_rec, int length,
                            long long position)
{
    int i=-1;
    long long pos;
    long long cur_pos;
    uchar *cmp_rec;
    uchar deleted = 1;

    DBUG_ENTER("Spartan_data::delete_row");
    if(position == 0)
        position = header_size;
    pos = position;


    if (position == -1)
    {
        cmp_rec = (uchar*)my_malloc(length, MYF(MY_ZEROFILL|MY_WME));
        pos = 0;
        cur_pos = my_seek(data_file, header_size, MY_SEEK_SET, MYF(0));

        while((cur_pos != -1) && (pos !=-1))
        {
            pos = read_row(cmp_rec, length, cur_pos);
            if (memcmp(old_rec, cmp_rec, length) == 0)
            {
                number_records--;
                number_del_records++;
                pos = cur_pos;
                cur_pos = -1;
            }else
                cur_pos = cur_pos + length + record_header_size;
        }
        my_free(cmp_rec);
    }

    if (pos != -1)
    {
        pos = my_seek(data_file, pos, MY_SEEK_SET, MYF(0));
        /*重新写入一个标记位*/
        i = my_write(data_file, &deleted, sizeof(uchar), MYF(0));
        i = (i>1)?0:i;
    }

    write_header();
    DBUG_RETURN(i);
}

int Spartan_data::read_row(uchar *buf, int length, long long position)
{
    int i;
    int rec_len;
    long long pos;
    uchar deleted = 2;

    DBUG_ENTER("Spartan_data::read_row");

    if(position<=0)
        position = header_size;
    pos = my_seek(data_file, position, MY_SEEK_SET, MYF(0));

    if(pos != -1)
    {
        i = my_read(data_file, &deleted, sizeof(uchar), MYF(0));
        if(deleted == 0)
        {
            /*deleted =0 说明没有被删除*/
            /*读取记录的长度*/
            i = my_read(data_file, (uchar*)&rec_len, sizeof(int), MYF(0));
            /*读取记录的内容*/
            i = my_read(data_file,buf, 
                    (length<rec_len? length : rec_len), MYF(0));
        }else if(i == 0){
            /*读取失败*/
            DBUG_RETURN(-1);
        }else{
            /*记录被删除*/
            DBUG_RETURN(read_row(buf,length, cur_position()+length+(record_header_size-sizeof(uchar))));
        }
    }else
        DBUG_RETURN(-1);

    DBUG_RETURN(0);
}

int Spartan_data::close_table()
{
    DBUG_ENTER("Spartan_data::close_table");
    if(data_file != -1)
    {
        my_close(data_file, MYF(0));
        data_file = -1;
    }
    DBUG_RETURN(0);
}

int Spartan_data::records()
{
    DBUG_ENTER("Spartan_data::records");
    DBUG_RETURN(0);
}

int Spartan_data::del_records()
{
    DBUG_ENTER("Spartan_data::del_records");
    DBUG_RETURN(0);
}

long long Spartan_data::cur_position()
{
    long long pos;
    DBUG_ENTER("Spartan_data::cur_position");
    pos = my_seek(data_file, 0L, MY_SEEK_CUR, MYF(0));
    if(pos == 0)
        DBUG_RETURN(header_size);
    DBUG_RETURN(pos);
}

int Spartan_data::trunc_table()
{
    DBUG_ENTER("Spartan_data::trunc_table");
    if(data_file != -1)
    {
        my_chsize(data_file, 0, 0, MYF(MY_WME));
        write_header();
    }
    DBUG_RETURN(0);
}

int Spartan_data::row_size(int length)
{
    DBUG_ENTER("Spartan_data::row_size");
    DBUG_RETURN(length + record_header_size);
}

