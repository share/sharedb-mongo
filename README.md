# sharedb-mongo

  [![NPM Version](https://img.shields.io/npm/v/sharedb-mongo.svg)](https://npmjs.org/package/sharedb-mongo)
  [![Build Status](https://travis-ci.org/share/sharedb-mongo.svg?branch=master)](https://travis-ci.org/share/sharedb-mongo)
  [![Coverage Status](https://coveralls.io/repos/github/share/sharedb-mongo/badge.svg?branch=master)](https://coveralls.io/github/share/sharedb-mongo?branch=master)

MongoDB database adapter for [sharedb](https://github.com/share/sharedb). This
driver can be used both as a snapshot store and oplog.

Snapshots are stored where you'd expect (the named collection with _id=id). In
addition, operations are stored in `o_COLLECTION`. For example, if you have
a `users` collection, the operations are stored in `o_users`.

JSON document snapshots in sharedb-mongo are unwrapped so you can use mongo
queries directly against JSON documents. (They just have some extra fields in
the form of `_v` and `_type`). It is safe to query documents directly with the
MongoDB driver or command line. Any read only mongo features, including find,
aggregate, and map reduce are safe to perform concurrent with ShareDB.

However, you must *always* use ShareDB to edit documents. Never use the
MongoDB driver or command line to directly modify any documents that ShareDB
might create or edit. ShareDB must be used to properly persist operations
together with snapshots.


## Usage

`sharedb-mongo` wraps native [mongodb](https://github.com/mongodb/node-mongodb-native), and it supports the same configuration options.

There are two ways to instantiate a sharedb-mongo wrapper:

1. The simplest way is to invoke the module and pass in your mongo DB
arguments as arguments to the module function. For example:

```javascript
const db = require('sharedb-mongo')('mongodb://localhost:27017/test');
const backend = new ShareDB({db});
```

2. If you'd like to reuse a mongo db connection or handle mongo driver
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

## Error codes

Mongo errors are passed back directly. Additional error codes:

#### 4100 -- Bad request - DB

* 4101 -- Invalid op version
* 4102 -- Invalid collection name
* 4103 -- $where queries disabled
* 4104 -- $mapReduce queries disabled
* 4105 -- $aggregate queries disabled
* 4106 -- $query property deprecated in queries
* 4107 -- Malformed query operator
* 4108 -- Only one collection operation allowed
* 4109 -- Only one cursor operation allowed
* 4110 -- Cursor methods can't run after collection method

#### 5100 -- Internal error - DB

* 5101 -- Already closed
* 5102 -- Snapshot missing last operation field
* 5103 -- Missing ops from requested version
* 5104 -- Failed to parse query


## MIT License
Copyright (c) 2015 by Joseph Gentle and Nate Smith

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
