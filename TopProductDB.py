import sqlite3
import hashlib

class Database():

    def __init__(self, path_db):
        self.con = sqlite3.connect(path_db)
        self.c = self.con.cursor()

    def close(self):
        self.con.close()


class DatabaseTopProduct(Database):
    def create_table(self):
        self.c.execute('''
        create table top_product (
            uid integer primary key,
            data_json text,
            top5 text
        )
    ''')

    def insert(self, uid, data_json, top5):
        self.c.execute('insert or replace into top_product values (?,?,?)', (uid, data_json, top5))
        self.con.commit()

    def insert_bulk(self, list_data):
        self.c.executemany('insert into top_product values (?,?,?)', list_data)
        self.con.commit()

    def get_top5_product(self, uid):
        result = self.c.execute('select top5 from top_product where uid = ?', (uid,)).fetchone()
        return result[0] if(result) else None

    def get_json_product(self, uid):
        result = self.c.execute('select data_json from top_product where uid = ?', (uid,)).fetchone()
        return result[0] if(result) else None


class DatabaseMetadata(Database):
    def create_table(self):
        self.c.execute('''
            create table metadata (
                md5_user varchar(32),
                md5_product varchar(32),
                shard integer
            )
        ''')

    def insert(self, path_user, path_product, shard):
        self.c.execute('insert into metadata values (?,?,?)', (
            hashlib.md5(open(path_user,'rb').read()).hexdigest(),
            hashlib.md5(open(path_product,'rb').read()).hexdigest(),
            shard
        ))
        self.con.commit()

    def isMetadataExists(self, path_user, path_product, shard):
        exists = self.c.execute('select 1 from metadata where md5_user = ? and md5_product = ? and shard = ?', (
            hashlib.md5(open(path_user,'rb').read()).hexdigest(),
            hashlib.md5(open(path_product,'rb').read()).hexdigest(),
            shard
        ,)).fetchone()
        return True if(exists) else False

