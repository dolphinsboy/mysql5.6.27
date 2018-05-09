#include "spartan_index.h"
#include <my_dir.h>
#include <string.h>

Spartan_index::Spartan_index(int keylen)
{
    root = NULL;
    crashed = false;
    max_key_len = keylen;
    index_file = -1;
    block_size = max_key_len + sizeof(long long) + sizeof(int);
}

Spartan_index::Spartan_index()
{
    root = NULL;
    crashed = false;
    max_key_len = -1;
    index_file = -1;
    block_size = -1;
}

Spartan_index::~Spartan_index(void)
{

}

int Spartan_index::create_index(char *path, int keylen)
{
    DBUG_ENTER("Spartan_index::create_index");
    DBUG_PRINT("info", ("path:%s", path));

    open_index(path);
    max_key_len = keylen;

    block_size = max_key_len + sizeof(long long);
    write_header();
    DBUG_RETURN(0);
}

int Spartan_index::open_index(char *path)
{
    DBUG_ENTER("Spartan_index::open_index");
    index_file = my_open(path, O_RDWR|O_CREAT|O_BINARY|O_SHARE, MYF(0));
    if(index_file == -1)
        DBUG_RETURN(errno);
    read_header();
    DBUG_RETURN(0);
}

int Spartan_index::read_header()
{
    DBUG_ENTER("Spartan_index::read_header");
    int i;
    if (block_size == -1)
    {
        my_seek(index_file, 0l, MY_SEEK_SET, MYF(0));
        i = my_read(index_file, (uchar*)&max_key_len, sizeof(int), MYF(0));
        
        block_size = max_key_len + sizeof(long long) + sizeof(int);
        i = my_read(index_file, (uchar*)&crashed, sizeof(bool), MYF(0));
    }else
    {
        i = (int)my_seek(index_file, sizeof(int) + sizeof(bool), MY_SEEK_SET, MYF(0));
    }
    DBUG_RETURN(0);
}

int Spartan_index::write_header()
{
    int i;
    DBUG_ENTER("Spartan_index::write_header");

    if (block_size != -1)
    {
        my_seek(index_file, 0l, MY_SEEK_SET, MYF(0));
        i = my_write(index_file, (uchar*)&max_key_len, sizeof(int), MYF(0));
        i = my_write(index_file, (uchar*)&crashed, sizeof(bool), MYF(0));
    }

    DBUG_RETURN(0);
}

/*将SDE_INDEX结构写入到索引文件中*/
long long Spartan_index::write_row(SDE_INDEX *ndx)
{
    long long pos;
    int i;
    int len;
    DBUG_ENTER("Spartan_index::write_row");

    /*追加写*/
    pos = my_seek(index_file, 0l, MY_SEEK_END, MYF(0));

    /*写val*/
    i = my_write(index_file, ndx->key, max_key_len, MYF(0));
    memcpy(&pos,&ndx->pos, sizeof(long long));

    /*写pos信息*/
    i += my_write(index_file, (uchar*)&pos, sizeof(long long), MYF(0));
    memcpy(&len, &ndx->length, sizeof(int));

    /*写key len*/
    i += my_write(index_file, (uchar*)&pos, sizeof(long long), MYF(0));
    if (i == -1)
        pos = i;
    DBUG_RETURN(pos);
}

/*从索引文件读取索引记录生成SDE_INDEX结构体*/
SDE_INDEX *Spartan_index::read_row(long long Position)
{
   int i;
   long long pos;

   SDE_INDEX *ndx = NULL;

   DBUG_ENTER("Spartan_index::read_row");

   pos = my_seek(index_file, (ulong) Position, MY_SEEK_SET, MYF(0));

   if (pos != -1)
   {
       ndx = new SDE_INDEX();
       i = my_read(index_file, ndx->key, max_key_len, MYF(0));
       i = my_read(index_file,(uchar*)&ndx->pos, sizeof(long long), MYF(0));

       if (i == -1)
       {
           delete ndx;
           ndx = NULL;
       }
   }
   DBUG_RETURN(ndx);
}

/*在内存将一个index key插入索引中*/
int Spartan_index::insert_key(SDE_INDEX *ndx, bool allow_dupes)
{
    SDE_NDX_NODE *p = NULL;
    SDE_NDX_NODE *n = NULL;
    SDE_NDX_NODE *o = NULL;

    int i = -1;
    int icmp;

    bool dupes = false;
    bool done = false;

    DBUG_ENTER("Spartan_index::insert_key");

    if(root == NULL)
    {
        /*新索引,初始化root*/
        root = new SDE_NDX_NODE();
        root->next = NULL;
        root->prev = NULL;

        memcpy(root->key_ndx.key, ndx->key, max_key_len);
        root->key_ndx.pos = ndx->pos;
        root->key_ndx.length = ndx->length;
    }else
        p = root;

    while((p!=NULL) && !done)
    {
        icmp = memcmp(ndx->key, p->key_ndx.key,
                (ndx->length > p->key_ndx.length) ?
                ndx->length : p->key_ndx.length);

        if(icmp>0)
        {
            n = p;
            p = p->next;
        }else if(!allow_dupes && (icmp == 0))
        {
            /*已经存在*/
            p = NULL;
            dupes = true;
        }else
        {
            /*插在前面*/
            n = p->prev;
            done = true;
        }
    }

    if((n != NULL) && !dupes)
    {
        if (p == NULL)
        {
            /*插入到尾部*/
            p = new SDE_NDX_NODE();
            n->next = p;
            p->prev = n;
            memcpy(p->key_ndx.key, ndx->key, max_key_len);
            p->key_ndx.pos = ndx->pos;
            p->key_ndx.length = ndx->length;
        }
        else
        {
            /*在n和p之间插入o节点,n在前,p在后*/
            o = new SDE_NDX_NODE();
            memcpy(o->key_ndx.key, ndx->key, max_key_len);
            o->key_ndx.pos = ndx->pos;
            o->key_ndx.length = ndx->length;

            o->prev = n;
            o->next = p;
            n->next = o;
            p->prev = o;
        }
        i = 1;
    }

    DBUG_RETURN(i);
}


int Spartan_index::delete_key(uchar *buf, long long pos, int key_len)
{
    SDE_NDX_NODE *p;
    int icmp;
    int buf_len;
    bool done = false;

    DBUG_ENTER("Spartan_index::delete_key");
    p = root;

    /*找到需要删除的节点*/
    while ((p != NULL) && !done)
    {
        buf_len = p->key_ndx.length;
        icmp = memcmp(buf, p->key_ndx.key,
                (buf_len>key_len) ? buf_len : key_len);

        if(icmp == 0)
        {
            if(pos != -1)
            {
                if (pos == p->key_ndx.pos)
                    done = true;
            }else
                done = true;
        }else{
            p = p->next;
        }
    }

    /*双向链表删除节点p*/
    if(p != NULL)
    {
        if(p->next != NULL)
        {
            p->next->prev = p->prev;
        }else if(p->prev != NULL)
        {
            p->prev->next = p->next;
        }else
            root = p->next;
        delete p;
    }
    DBUG_RETURN(0);
}

int Spartan_index::update_key(uchar *buf, long long pos, int key_len)
{
    /*根据pos更新节点的key*/
    SDE_NDX_NODE *p;
    bool done = false;

    DBUG_ENTER("Spartan_index::update_key");
    p = root;

    while((p != NULL) && !done)
    {
        if(p->key_ndx.pos == pos)
            done = true;
        else
            p = p->next;
    }
    
    if(p!=NULL)
        memcpy(p->key_ndx.key, buf, key_len);

    DBUG_RETURN(0);
}

long long Spartan_index::get_index_pos(uchar *buf, int key_len)
{
    long long pos=-1;
    DBUG_ENTER("Spartan_index::get_index_pos");

    SDE_INDEX *ndx;
    ndx = seek_index(buf, key_len);
    if(ndx != NULL)
        pos = ndx->pos;
    DBUG_RETURN(pos);
}

uchar *Spartan_index::get_next_key()
{
    /*类似迭代器的功能*/
    uchar *key = 0;

    DBUG_ENTER("Spartan_index::get_next_key");

    if(range_ptr != NULL)
    {
        key = (uchar*)my_malloc(max_key_len, MYF(MY_ZEROFILL|MY_WME));
        memcpy(key, range_ptr->key_ndx.key, range_ptr->key_ndx.length);
        range_ptr = range_ptr->next;
    }

    DBUG_RETURN(key);
}

uchar *Spartan_index::get_prev_key()
{
    /*获取range_ptr前一个key*/
    
    uchar *key = 0;

    DBUG_ENTER("Spartan_index::get_prev_key");

    if(range_ptr != NULL)
    {
        key = (uchar*)my_malloc(max_key_len, MYF(MY_ZEROFILL|MY_WME));
        memcpy(key, range_ptr->key_ndx.key, range_ptr->key_ndx.length);
        range_ptr = range_ptr->prev;
    }

    DBUG_RETURN(key);
}

uchar *Spartan_index::get_first_key()
{
    SDE_NDX_NODE *n = root;
    uchar *key = 0;

    DBUG_ENTER("Spartan_index::get_first_key");

    if(root != NULL)
    {
        key = (uchar*)my_malloc(max_key_len, MYF(MY_ZEROFILL|MY_WME));
        memcpy(key, n->key_ndx.key,n->key_ndx.length);
    }
    
    DBUG_RETURN(key);
}

uchar *Spartan_index::get_last_key()
{
    SDE_NDX_NODE *n = root;
    uchar *key = 0;

    DBUG_ENTER("Spartan_index::get_last_key");

    while(n->next!=NULL)
        n = n->next;

    if(n != NULL)
    {
        key = (uchar*)my_malloc(max_key_len, MYF(MY_ZEROFILL|MY_WME));
        memcpy(key, n->key_ndx.key, n->key_ndx.length);
    }

    DBUG_RETURN(key);
}

int Spartan_index::close_index()
{
    SDE_NDX_NODE *p;
    DBUG_ENTER("Spartan_index::close_index");

    /*关闭索引文件*/
    if(index_file != -1)
    {
        my_close(index_file, MYF(0));
        index_file = -1;
    }

    /*清理内存的双向链表*/
    while(root != NULL)
    {
        p = root;
        root = root->next;
        delete p;
    }

    DBUG_RETURN(0);
}

SDE_INDEX *Spartan_index::seek_index(uchar *key, int key_len)
{
    SDE_INDEX *ndx = NULL;
    SDE_NDX_NODE *n = root;
    int buf_len;
    bool done = false;

    DBUG_ENTER("Spartan_index::seek_index");

    if(n != NULL)
    {
        while((n != NULL) && !done)
        {
            buf_len = n->key_ndx.length;
            if(memcmp(key, n->key_ndx.key,
                (buf_len>key_len) ? buf_len : key_len) == 0)
                done=true;
            else
                n = n->next;
        }
    }

    if(n != NULL)
    {
        ndx = &n->key_ndx;
        range_ptr = n;
    }

    DBUG_RETURN(ndx);
}

SDE_NDX_NODE *Spartan_index::seek_index_pos(uchar *key, int key_len)
{
    SDE_NDX_NODE *n = root;
    int buf_len;
    bool done = false;

    DBUG_ENTER("Spartan_index::seek_index_pos");
    if(n != NULL)
    {
        while((n->next != NULL) && !done)
        {
            buf_len = n->key_ndx.length;
            if(memcmp(key, n->key_ndx.key,
                (buf_len>key_len) ? buf_len : key_len) == 0)
                done = true;
            else if(n->next != NULL)
                n = n->next;
        }
    }

    DBUG_RETURN(n);
}

int Spartan_index::load_index()
{
    SDE_INDEX *ndx;
    int i = 1;
    DBUG_ENTER("Spartan_index::load_index");

    if(root != NULL)
        destroy_index();

    read_header();

    while(i != 0)
    {
        ndx = new SDE_INDEX();
        i = my_read(index_file,(uchar*)&ndx->key, max_key_len, MYF(0));
        i = my_read(index_file,(uchar*)&ndx->pos, sizeof(long long), MYF(0));
        i = my_read(index_file,(uchar*)&ndx->length, sizeof(long long), MYF(0));

        if (i != 0)
            insert_key(ndx, false);
    }

    DBUG_RETURN(0);
}


long long Spartan_index::curfpos()
{
    long long pos = 0;
    DBUG_ENTER("Spartan_index::curfpos");
    pos = my_seek(index_file, 0l, MY_SEEK_CUR, MYF(0));
    DBUG_RETURN(pos);
}

int Spartan_index::save_index()
{
    /*将内存的双向链表刷到文件中*/
    SDE_NDX_NODE *n = NULL;
    int i;

    DBUG_ENTER("Spartan_index::save_index");
    /*清空index_file文件*/
    i = my_chsize(index_file, 0l, '\n', MYF(MY_WME));

    /*写索引文件头*/
    write_header();

    /*遍历双向链表,写入索引文件*/
    n = root;
    while(n != NULL)
    {
        write_row(&n->key_ndx);
        n = n->next;
    }

    DBUG_RETURN(0);
}

int Spartan_index::destroy_index()
{
    /*清空内存中的双向链表*/
    SDE_NDX_NODE *n = root;
    DBUG_ENTER("Spartan_index::destroy_index");

    while(root != NULL)
    {
        n = root;
        root = root->next;
        delete n;
    }
    root = NULL;
    DBUG_RETURN(0);
}

long long Spartan_index::get_first_pos()
{
    long long pos = -1;

    DBUG_ENTER("Spartan_index::get_first_pos");

    if(root != NULL)
        pos = root->key_ndx.pos;
    DBUG_RETURN(pos);
}

int Spartan_index::trunc_index()
{
    DBUG_ENTER("Spartan_index::trun_index");

    if(index_file != -1)
    {
        my_chsize(index_file, 0L, 0, MYF(MY_WME));
        write_header();
    }

    DBUG_RETURN(0);
}
