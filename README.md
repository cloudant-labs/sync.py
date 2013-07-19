sync.py
=======

**Playground** for sync ideas. 

To run the demo:

    python tests/demo.py

## Current features
 * store json docs
 * define secondary indexes on said doc
 * define collections
 * retrieve docs by id, index, collection
 * execute arbitrary SQL

## Wishlist
 * Pagination of index/collection result set
 * Sub-query on collection/index (e.g. `select * from collection where ...`) 
 * Declarative indexing
   * e.g. define secondary indexes on first level field 
   * what limitations make this tractable?
 * Register/define collection, interactions with collections 
 * Register/define indexes, interactions with indexes
 * Sync ;)
