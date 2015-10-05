# livedb-mongo

MongoDB database adapter for [livedb](https://github.com/share/livedb). This
driver can be used both as a snapshot store and oplog.

Snapshots are stored where you'd expect (the named collection with _id=docName).
Operations are stored in `COLLECTION_ops`. If you have a users collection,
the operations are stored in `users_ops`. If you have a document called `fred`,
operations will be stored in documents called `fred v0`, `fred v1`, `fred v2`,
and so on.

JSON document snapshots in livedb-mongo are unwrapped so you can use mongo
queries directly against JSON documents. (They just have some extra fields in
the form of `_v` and `_type`). You should always use livedb to edit documents--
don't just edit them directly in mongo. You'll get weird behaviour if you do.

## Usage

LiveDB-mongo wraps native
[mongodb](https://github.com/mongodb/node-mongodb-native). It passes all the
arguments straight to mongodb's mongoClient's connect-function. `npm install
livedb-mongo` then create your database wrapper using the same arguments you
would pass to mongodb driver:

```javascript
var livedbmongo = require('livedb-mongo');
var mongo = livedbmongo('localhost:27017/test');

var livedb = require('livedb').client(mongo); // Or whatever. See livedb's docs.
```

If you prefer, you can instead create a mongodb instance yourself and pass it
to livedb-mongo:

```javascript

var mongoClient = require('mongodb').MongoClient;
var livedbmongo = require('livedb-mongo');

mongoClient.connect('localhost:27017/test', function(err, db){
  var mongo = require('livedb-mongo')(db);
  var livedb = require('livedb').client(mongo); // Or whatever. See livedb's docs.
});

## Error codes

Mongo errors are passed back directly. Additional error codes:

#### 4100 -- Bad request - DB

* 4101 -- Invalid op version
* 4102 -- Invalid collection name
* 4103 -- $where queries disabled
* 4104 -- $mapReduce queries disabled
* 4105 -- $aggregate queries disabled

#### 5100 -- Internal error - DB

* 5101 -- Already closed
* 5102 -- Snapshot missing last operation field

```

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
