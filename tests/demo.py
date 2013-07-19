from cloudantsync.datastore import DataStore

ds = DataStore()
print 'test indexes:', ds.indexes()

def my_index(item):
    return {'key': item.get('foo', 'no foo'), 'value': 1}

item1 = {
    "_id": "123",
    "foo": "bar"
}

item2 = {
    "baz": "bat"
}

item3 = {
    "foo": "bar"
}

item4 = {
    "foo": "bat"
}

print 'test set:', ds.set(item1, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item2, indexers={"my_index": my_index}, collection='evens')
print 'test set:', ds.set(item2, indexers={"my_index": my_index}, collection='evens')
print 'test set:', ds.set(item2, indexers={"my_index": my_index}, collection='evens')
print 'test set:', ds.set(item3, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item3, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item3, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item3, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item3, indexers={"my_index": my_index}, collection='odds')
print 'test set:', ds.set(item4, indexers={"my_index": my_index}, collection='evens')
print 'test set:', ds.set(item4, indexers={"my_index": my_index}, collection='evens')

doc_id = ds.set(item4, indexers={"my_index": my_index}, collection='evens')

print 'test get:', ds.get("123")
print 'test get:', ds.get(doc_id)

print 'test by index:', ds.get_by_index("my_index")[0:2]

# Query by arbitrary index
query = "select key, sum(value) from t_cloudant_sync_idx_my_index group by key;"
print 'test by index:', \
    ds.__fetch_query__(sql=query, fields=('key', 'sum'))[0:2]

# Query by collection
print 'test by collection:', ds.get_collection('evens')[0:2]
