# sharedb-mongo

[![NPM Version](https://img.shields.io/npm/v/sharedb-mongo.svg)](https://npmjs.org/package/sharedb-mongo)
[![Test](https://github.com/share/sharedb-mongo/workflows/Test/badge.svg)](https://github.com/share/sharedb-mongo/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/share/sharedb-mongo/badge.svg?branch=master)](https://coveralls.io/github/share/sharedb-mongo?branch=master)

MongoDB database adapter for [sharedb](https://github.com/share/sharedb). This
driver can be used both as a snapshot store and oplog.

Snapshots are stored where you'd expect (the named collection with \_id=id). In
addition, operations are stored in `o_COLLECTION`. For example, if you have
a `users` collection, the operations are stored in `o_users`.

JSON document snapshots in sharedb-mongo are unwrapped so you can use mongo
queries directly against JSON documents. (They just have some extra fields in
the form of `_v` and `_type`). It is safe to query documents directly with the
MongoDB driver or command line. Any read only mongo features, including find,
aggregate, and map reduce are safe to perform concurrent with ShareDB.

However, you must _always_ use ShareDB to edit documents. Never use the
MongoDB driver or command line to directly modify any documents that ShareDB
might create or edit. ShareDB must be used to properly persist operations
together with snapshots.

## Usage

`sharedb-mongo` uses the [MongoDB NodeJS Driver](https://github.com/mongodb/node-mongodb-native), and it supports the same configuration options.

There are two ways to instantiate a sharedb-mongo wrapper:

1.  The simplest way is to invoke the module and pass in your mongo DB
    arguments as arguments to the module function. For example:

        ```javascript
        const db = require('sharedb-mongo')('mongodb://localhost:27017/test', {mongoOptions: {...}});
        const backend = new ShareDB({db});
        ```

2.  If you'd like to reuse a mongo db connection or handle mongo driver
    instantiation yourself, you can pass in a function that calls back with
    a mongo instance.

        ```javascript
        const mongodb = require('mongodb');
        const db = require('sharedb-mongo')({mongo: function(callback) {
          mongodb.connect('mongodb://localhost:27017/test', callback);
        }});
        const backend = new ShareDB({db});
        ```

## Queries

In ShareDB, queries are represented as single JavaScript objects. But
Mongo exposes methods on collections and cursors such as `mapReduce`,
`sort` or `count`. These are encoded into ShareDBMongo's query object
format through special `$`-prefixed keys that are interpreted and
stripped out of the query before being passed into Mongo's `find`
method.

Here are some examples:

| MongoDB query code                                   | ShareDBMongo query object                           |
| ---------------------------------------------------- | --------------------------------------------------- |
| `coll.find({x: 1, y: {$ne: 2}})`                     | `{x: 1, y: {$ne: 2}}`                               |
| `coll.find({$or: [{x: 1}, {y: 1}])`                  | `{$or: [{x: 1}, {y: 1}]}}`                          |
| `coll.mapReduce({map: ..., reduce: ...})`            | `{$mapReduce: {map: ..., reduce: ...}`              |
| `coll.find({x: 1}).sort({y: -1})`                    | `{x: 1, $sort: {y: -1}}`                            |
| `coll.find().limit(5).count({applySkipLimit: true})` | `{x: 1, $limit: 5, $count: {applySkipLimit: true}}` |

Most of Mongo 3.2's
[collection](https://docs.mongodb.com/manual/reference/method/js-collection/)
and
[cursor](https://docs.mongodb.com/manual/reference/method/js-cursor/)
methods are supported. Methods calls map to query properties whose key
is the method name prefixed by `$` and value is the argument passed to
the method. `$readPref` is an exception -- it takes an object with
`mode` and `tagSet` fields which map to the two arguments passed into
the `readPref` method.

For a full list of supported collection and cursor methods, see
`collectionOperationsMap`, `cursorTransformsMap` and
`cursorOperationsMap` in index.js

## `getOps` without strict linking

There is a `getOpsWithoutStrictLinking` flag, which can be set to
`true` to speed up `getOps` under certain circumstances, but with
potential risks to the integrity of the results. Read below for
more detail.

### Introduction

ShareDB has to deal with concurrency issues. In particular, here we
discuss the issue of submitting multiple competing ops against a
version of a document.

For example, if I have a version of a document at v1, and I
simultaneously submit two ops (from different servers, say) against
this snapshot, then we need to handle the fact that only one of
these ops can be accepted as canonical and applied to the snapshot.

This issue is dealt with through **optimistic locking**. Even if
you are only asking for a subset of the ops, under the default
behaviour, `getOps` will fetch **all** the ops up to the current
version.

### Optimistic locking and linked ops

`sharedb-mongo` deals with its concurrency issue with multiple op
submissions with optimistic locking. Here's an example of its
behaviour:

- my doc exists at v1
- two simultaneous v1 ops are submitted to ShareDB
- both ops are committed to the database
- one op is applied to the snapshot, and the updated snapshot is
  written to the database
- the second op finds that its updated snapshot conflicts with
  the committed snapshot, and the snapshot is rejected, but the
  committed op **remains in the database**

In reality, `sharedb-mongo` attempts to clean up this failed op,
but there's still the small chance that the server crashes
before it can do so, meaning that we may have multiple ops
lingering in the database with the same version.

Because some non-canonical ops may exist in the database, we
cannot just perform a naive fetch of all the ops associated with
a document, because it may return multiple ops with the same
version (where one was successfully applied, and one was not).

In order to return a valid set of canonical ops, the optimistic
locking has a notion of **linked ops**. That is, each op will
point back to the op that it built on top of, and ultimately
the current snapshot points to the op that committed it to the
database.

Because of this, we can work backwards from the current snapshot,
following the trail of op links all the way back to get a chain
of canonical, valid, linked ops. This way, even if a spurious
op exists in the database, no other op will point to it, and it
will be correctly ignored.

This approach has a big down-side: it forces us to fetch all the
ops up to the current version. This might be fine if you want
all ops, or are fetching very recent ops, but can have a large
impact on performance if you only want ops 1-10 of a 10,000
op document, because you actually have to fetch all the ops.

### Dropping strict linking

In order to speed up the performance of `getOps`, you can set
`getOpsWithoutStrictLinking: true`. This will attempt to fetch
the bare minimum ops, whilst still trying to maintain op
integrity.

The assumption that underpins this approach is that any op
that exists with a unique combination of `d` (document ID)
and `v` (version), **is a valid op**. In other words, it
had no conflicts and can be considered canonical.

Consider a document with some ops, including some spurious,
failed ops:

- v1: unique
- v2: unique
- v3: collision 3
- v3: collision 3
- v4: collision 4
- v4: collision 4
- v5: unique
- v6: unique
  ...
- v1000: unique

If I want to fetch ops v1-v3, then we:

- look up v4
- find that v4 is not unique
- look up v5
- see that v5 is unique and therefore assumed valid
- look backwards from v5 for a chain of valid ops, avoiding
  the spurious commits for v4 and v3.
- This way we don't need to fetch all the ops from v5 to the
  current version.

In the case where a valid op cannot be determined, we still
fall back to fetching all ops and working backwards from the
current version.

### Middlewares

Middlewares let you hook into the `sharedb-mongo` pipeline for certain actions. They are distinct from [middleware in `ShareDB`](https://github.com/share/sharedb) as they are closer to the concrete calls that are made to `MongoDB` itself.

The original intent for middleware on `sharedb-mongo` is to support running in a sharded `MongoDB` cluster to satisfy the requirements on shard keys for versions 4.2 and greater of `MongoDB`. For more information see [the MongoDB docs](https://docs.mongodb.com/manual/core/sharding-shard-key/#shard-keys).

#### Usage

`share.use(action, fn)`
Register a new middleware.

- `action` _(String)_
  One of:
  - `'beforeCreate'`: directly before the call to write a new document
  - `'beforeOverwrite'`: directly before the call to replace a document, can include edits as well as deletions
  - `'beforeSnapshotLookup'`: directly before the call to issue a query for one or more snapshots by ID
- `fn` _(Function(context, callback))_
  Call this function at the time specified by `action`
  - `context` will always have the following properties:
    - `action`: The action this middleware is handling
    - `collectionName`: The collection name being handled
    - `options`: Original options as they were passed into the relevant function that triggered the action
    - `'beforeCreate'` actions have additional context properties:
      - `documentToWrite` - The document to be written
      - `op` - The op that represents the changes that will be made to the document
    - `'beforeOverwrite'` actions have additional context properties:
      - `documentToWrite` - The document to be written
      - `op` - The op that represents the changes that will be made to the document
      - `query` - A filter that will be used to lookup the document that is about to be edited, which should always include an ID and snapshot version e.g. `{_id: 'uuid', _v: 1}`
    - `'beforeSnapshotLookup'` actions have additional context properties:
      - `query` - A filter that will be used to lookup the snapshot. When a single snapshot is looked up the query will take the shape `{_id: docId}` while a bulk lookup by a list of IDs will resemble `{_id: {$in: docIdsArray}}`.
      - `findOptions` - Middleware can define and populate this object on the context to pass options to the MongoDB driver when doing the lookup.

### Limitations

#### Integrity

Attempting to infer a canonical op can be dangerous compared
to simply following the valid op chain from the snapshot,
which is - by definition - canonical.

This alternative behaviour should be safe, but should be used
with caution, because we are attempting to _infer_ a canonical
op, which may have unforeseen corner cases that return an
**invalid set of ops**.

This may be especially true if the ops are modified outside
of `sharedb-mongo` (eg by setting a TTL, or manually updating
them).

#### Recent ops

There are cases where this flag may slow down behaviour. In
the case of attempting to fetch very recent ops, setting this
flag may make extra database round-trips where fetching the
snapshot would have been faster.

#### `getOpsBulk` and `getOpsToSnapshot`

This flag **only** applies to `getOps`, and **not** to the
similar `getOpsBulk` and `getOpsToSnapshot` methods, whose
performance will remain unchanged.

## Error codes

Mongo errors are passed back directly. Additional error codes:

#### 4100 -- Bad request - DB

- 4101 -- Invalid op version
- 4102 -- Invalid collection name
- 4103 -- $where queries disabled
- 4104 -- $mapReduce queries disabled
- 4105 -- $aggregate queries disabled
- 4106 -- $query property deprecated in queries
- 4107 -- Malformed query operator
- 4108 -- Only one collection operation allowed
- 4109 -- Only one cursor operation allowed
- 4110 -- Cursor methods can't run after collection method

#### 5100 -- Internal error - DB

- 5101 -- Already closed
- 5102 -- Snapshot missing last operation field
- 5103 -- Missing ops from requested version
- 5104 -- Failed to parse query
