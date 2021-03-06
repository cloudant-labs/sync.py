import uuid
import sqlite3
import json


class DataStore():
    def __init__(self, store_file="datastore"):
        """
        Connect the database, check for existing 'indexes'.

            datastore: string, location of datastore (file or :memory:)
            fields: tuple (ordering important) of fields to store

        """
        self.datastore = sqlite3.connect(store_file)
        self.__indexes__ = []
        self.indexes()
        self.__create_table__('datastore', table_type='master')
        self.__create_table__('collections')

    def __create_table__(self, name, table_type='index'):
        """
        Make a table
        """
        table_name = 'cloudant_sync_%s' % name
        table_sql = """CREATE table if not exists t_%s
        (_id text, _rev text, body text);"""
        # TODO: index on _id and _rev (maybe)
        indexes = ["CREATE index if not exists idx_%s_id on t_%s (_id)"]
        if table_type is 'index':
            table_name = 'cloudant_sync_idx_%s' % name
            table_sql = """CREATE table if not exists t_%s
            (_id text, key text, value text);"""
            indexes.append("CREATE index if not exists idx_%s_key on t_%s (key)")
        cursor = self.datastore.cursor()
        cursor.execute(table_sql % table_name)
        for idx_sql in indexes:
            print idx_sql % tuple([table_name] * 2)
            cursor.execute(idx_sql % tuple([table_name] * 2))
        self.datastore.commit()

    def __update_indexes__(self, item, indexers):
        """
        TODO: look at using the sqlite create function thing here...
        """
        for name, index_function in indexers.items():
            if 't_cloudant_sync_idx_%s' % name not in self.__indexes__:
                self.__create_table__(name)
                self.__indexes__.append(name)

            row = {
                'id': item['_id'],
            }
            row.update(index_function(item))
            self.__set_index__(row, name)

    def __set_index__(self, row, index):
        """
        Add an id, key, value triple to an index table
        """
        sql = "INSERT INTO t_cloudant_sync_idx_%s VALUES ('%s', '%s', '%s');"
        cursor = self.datastore.cursor()
        cursor.execute(sql % (index, row['id'], row['key'], row['value']))

    def __fetch_query__(self, sql, fields=('id', 'key', 'value')):
        docs = []
        cursor = self.datastore.cursor()
        cursor.execute(sql)
        for i in cursor.fetchall():
            # NOTE: not the reserved _id because this is a reference
            docs.append(dict(zip(fields, i)))
        return docs

    def get(self, item_id):
        """
        Get an item from the data store
        """
        sql = "select body from t_cloudant_sync_datastore WHERE _id='%s';"
        cursor = self.datastore.cursor()
        cursor.execute(sql % item_id)
        return json.loads(cursor.fetchone()[0])

    def set(self, item, collection=None, indexers={}):
        """
        Add an item to the data store, and create necessary indexes.
        """
        if '_id' not in item.keys():
            item['_id'] = str(uuid.uuid4())
        if '_rev' not in item.keys():
            # TODO: correct for MVCC semantics
            item['_rev'] = 0
        self.__update_indexes__(item, indexers)

        sql = "INSERT INTO t_cloudant_sync_datastore VALUES ('%s', '%s', '%s');"
        cursor = self.datastore.cursor()
        cursor.execute(sql % (
            item['_id'],
            item['_rev'],
            json.dumps(item)
        ))
        if collection:
            sql = "INSERT INTO t_cloudant_sync_idx_collections VALUES ('%s', '%s', null);"
            cursor.execute(sql % (
                item['_id'],
                collection
            ))

        self.datastore.commit()
        return item['_id']

    def bulk_set(self, list_of_items, indexers={}):
        """
        Add multiple documents to the data store.

        TODO
        """
        return []

    def get_by_index(self, index):
        """
        Retrieve documents from an index.
        """
        sql = "select * from t_cloudant_sync_idx_%s" % (index)
        return self.__fetch_query__(sql)

    def get_collection(self, collection):
        """
        Retrieve documents from a collection.
        """
        sql = "select _id from t_cloudant_sync_idx_collections where key='%s';"
        return self.__fetch_query__(sql=sql % collection)

    def indexes(self):
        """
        Resets and synchronises the list of indexes in the data store.
        """
        sql = """SELECT name FROM sqlite_master
            WHERE type = 'table' AND name like 't_cloudant_sync_idx_%'"""
        self.__indexes__ = [x['name'] for x in self.__fetch_query__(
            sql,
            fields=['name']
        )]
        return self.__indexes__
