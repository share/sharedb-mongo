var mongodb = require('mongodb');
var async = require('async');

var metaOperators = {
  $comment: true
, $explain: true
, $hint: true
, $maxScan: true
, $max: true
, $min: true
, $orderby: true
, $returnKey: true
, $showDiskLoc: true
, $snapshot: true
, $count: true
, $aggregate: true
};

var cursorOperators = {
  $limit: 'limit'
, $skip: 'skip'
};

/* There are two ways to instantiate a livedb-mongo wrapper.
 *
 * 1. The simplest way is to invoke the module and pass in your mongo DB
 * arguments as arguments to the module function. For example:
 *
 * var livedbMongo = require('livedb-mongo')('localhost:27017/test');
 *
 * 2. If you already have a mongo Db instance that you want to use, you
 * alternatively can pass it into livedb-mongo:
 *
 * var mongodb = require('mongodb');
 * mongodb.connect('localhost:27017/test', function(err, db){
 *   var livedbMongo = require('livedb-mongo')(db);
 * });
*/

module.exports = LiveDbMongo;

function LiveDbMongo(mongo, options) {
  // use without new
  if (!(this instanceof LiveDbMongo)){
    var obj = Object.create(LiveDbMongo.prototype);
    obj.constructor.apply(obj, arguments);
    return obj;
  }

  if (!options) options = {};

  // pollDelay is a dodgy hack to work around race conditions replicating the
  // data out to the polling target secondaries. If a separate db is specified
  // for polling, it defaults to 300ms
  this.pollDelay = (options.pollDelay != null) ? options.pollDelay :
    (options.mongoPoll) ? 300 : 0;

  // By default, we create indexes on any ops collection that is used
  this.disableIndexCreation = options.disableIndexCreation;

  // The getVersion() and getOps() methods depend on a collectionname_ops
  // collection, and that collection should have an index on the operations
  // stored there. I could ask people to make these indexes themselves, but
  // even I forgot on some of my collections, so the mongo driver will just do
  // it automatically. This approach will leak memory relative to the number of
  // collections you have, but if you've got thousands of mongo collections
  // you're probably doing something wrong.

  // Map from collection name -> true for op collections we've ensureIndex'ed
  this.opIndexes = {};

  // Allow $while and $mapReduce queries. These queries let you run arbitrary
  // JS on the server. If users make these queries from the browser, there's
  // security issues.
  this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || options.allowJavaScriptQuery || false;

  // Aggregate queries are less dangerous, but you can use them to access any
  // data in the mongo database.
  this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries;

  // Track whether the close method has been called
  this.closed = false;

  if (typeof mongo === 'string') {
    // We can only get the mongodb client instance in a callback, so
    // buffer up any requests received in the meantime
    this.mongo = null;
    this.mongoPoll = null;
    this.pendingConnect = [];
    this._connect(mongo, options);
  } else {
    this.mongo = mongo;
    this.mongoPoll = options.mongoPoll;
    this.pendingConnect = null;
  }
};

LiveDbMongo.prototype.implementsProjection = true;

LiveDbMongo.prototype.getCollection = function(cName, callback) {
  // Check the collection name
  var err = this.validateCollectionName(cName);
  if (err) return callback(err);
  // Gotcha: calls back sync if connected or async if not
  this.getDbs(function(err, mongo) {
    if (err) return callback(err);
    var collection = mongo.collection(cName);
    return callback(null, collection);
  });
};

LiveDbMongo.prototype._getCollectionPoll = function(cName, callback) {
  // Check the collection name
  var err = this.validateCollectionName(cName);
  if (err) return callback(err);
  // Gotcha: calls back sync if connected or async if not
  this.getDbs(function(err, mongo, mongoPoll) {
    if (err) return callback(err);
    var collection = (mongoPoll || mongo).collection(cName);
    return callback(null, collection);
  });
};

LiveDbMongo.prototype.getCollectionPoll = function(cName, callback) {
  if (this.pollDelay) {
    var self = this;
    setTimeout(function() {
      self._getCollectionPoll(cName, callback);
    }, this.pollDelay);
    return;
  }
  this._getCollectionPoll(cName, callback);
};

LiveDbMongo.prototype.getDbs = function(callback) {
  if (this.closed) return callback('db already closed');
  // We consider ouself ready to reply if this.mongo is defined and don't check
  // this.mongoPoll, since it is optional and is null by default. Thus, it's
  // important that these two properties are only set together synchronously
  if (this.mongo) return callback(null, this.mongo, this.mongoPoll);
  this.pendingConnect.push(callback);
};

LiveDbMongo.prototype._flushPendingConnect = function() {
  var pendingConnect = this.pendingConnect;
  this.pendingConnect = null;
  for (var i = 0; i < pendingConnect.length; i++) {
    pendingConnect[i](null, this.mongo, this.mongoPoll);
  }
};

LiveDbMongo.prototype._connect = function(mongo, options) {
  var self = this;
  // Create the mongo connection client connections if needed
  if (options.mongoPoll) {
    async.parallel({
      mongo: function(parallelCb) {
        mongodb.connect(mongo, options.mongoOptions, parallelCb);
      },
      mongoPoll: function(parallelCb) {
        mongodb.connect(options.mongoPoll, options.mongoPollOptions, parallelCb);
      }
    }, function(err, results) {
      // Just throw the error if we fail to connect, since we aren't
      // implementing a way to retry
      if (err) throw err;
      self.mongo = results.mongo;
      self.mongoPoll = results.mongoPoll;
      self._flushPendingConnect();
    });
    return;
  }
  mongodb.connect(mongo, options, function(err, db) {
    if (err) throw err;
    self.mongo = db;
    self._flushPendingConnect();
  });
};

LiveDbMongo.prototype.close = function(callback) {
  var self = this;
  this.getDbs(function(err, mongo, mongoPoll) {
    if (err) return callback(err);
    self.closed = true;
    var closeCb = (mongoPoll) ?
      function(err) {
        if (err) return callback(err);
        mongoPoll.close(callback);
      } :
      callback;
    mongo.close(closeCb);
  });
};


// **** Snapshot methods

LiveDbMongo.prototype.getSnapshot = function(cName, docName, projection, callback) {
  // This code depends on the document being stored in the efficient way (which is to say, we've
  // promoted all fields in mongo). This will only work properly for json documents - which happen
  // to be the only types that we really want projections for.
  this.getCollection(cName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: docName};
    var mongoProjection = getMongoProjection(projection);
    collection.findOne(query, mongoProjection, function(err, doc) {
      callback(err, castToSnapshot(doc));
    });
  });
};

LiveDbMongo.prototype.getSnapshots = function(cName, docNames, projection, callback) {
  this.getCollection(cName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: {$in: docNames}};
    var mongoProjection = getMongoProjection(projection);
    collection.find(query, mongoProjection).toArray(function(err, docs) {
      if (err) return callback(err);
      callback(err, docs.map(castToSnapshot));
    });
  });
};

LiveDbMongo.prototype.bulkGetSnapshot = function(requests, projections, callback) {
  var self = this;
  var results = {};

  function getSnapshots(cName, eachCb) {
    var cResult = results[cName] = {};
    var docNames = requests[cName];
    var projection = projections && projections[cName];
    self.getSnapshots(cName, docNames, projection, function(err, snapshots) {
      if (err) return eachCb(err);
      for (var i = 0; i < snapshots.length; i++) {
        var snapshot = snapshots[i];
        cResult[snapshot.docName] = snapshot;
      }
      eachCb();
    });
  }
  async.each(Object.keys(requests), getSnapshots, function(err) {
    if (err) return callback(err);
    callback(null, results);
  });
};

LiveDbMongo.prototype.writeSnapshot = function(cName, docName, data, callback) {
  this.getCollection(cName, function(err, collection) {
    if (err) return callback(err);
    if (data == null) {
      collection.remove({_id: docName}, callback);
      return;
    }
    var doc = castToDoc(docName, data);
    collection.update({_id: docName}, doc, {upsert: true}, callback);
  });
};


// **** Oplog methods

// Overwrite me if you want to change this behaviour.
LiveDbMongo.prototype.getOplogCollectionName = function(cName) {
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

LiveDbMongo.prototype.validateCollectionName = function(cName) {
  if (/_ops$/.test(cName) || cName === 'system') {
    return 'Invalid collection name ' + cName;
  }
};

// Get and return the op collection from mongo, ensuring it has the op index.
LiveDbMongo.prototype.getOpCollection = function(cName, callback) {
  var self = this;
  this.getDbs(function(err, mongo) {
    if (err) return callback(err);
    var name = self.getOplogCollectionName(cName);
    var collection = mongo.collection(name);
    // Given the potential problems with creating indexes on the fly, it might
    // be preferrable to disable automatic creation
    if (self.disableIndexCreation) {
      return callback(null, collection);
    }
    if (self.opIndexes[cName]) {
      return callback(null, collection);
    }
    // Note: Creating indexes automatically like this is quite dangerous in
    // production if we are starting with a lot of data and no indexes already.
    // If indexes are created as the first ops are added to a collection this
    // won't be a problem, but backup restores that don't restore indexes could
    // lead to lockup of the database. Perhaps there should be a safety
    // mechanism of some kind
    collection.ensureIndex({name: 1, v: 1}, true, function(err) {
      if (err) return callback(err);
      self.opIndexes[cName] = true;
      callback(null, collection);
    });
  });
};

LiveDbMongo.prototype.writeOp = function(cName, docName, opData, callback) {
  if (typeof opData.v !== 'number') {
    var err = 'Invalid op version ' + cName + ' ' + docName + ' ' + opData.v;
    return callback(err);
  }
  this.getOpCollection(cName, function(err, opCollection) {
    if (err) return callback(err);
    var data = shallowClone(opData);
    data._id = docName + ' v' + opData.v,
    data.name = docName;
    opCollection.save(data, callback);
  });
};

LiveDbMongo.prototype.getVersion = function(cName, docName, callback) {
  var self = this;
  this.getOpCollection(cName, function(err, opCollection) {
    if (err) return callback(err);
    // Return the version from the latest op if there is one
    var opQuery = {
      $query: {name: docName},
      $orderby: {v: -1}
    };
    var opProjection = {_id: 0, v: 1};
    opCollection.findOne(opQuery, opProjection, function(err, op) {
      if (err) return callback(err);
      if (op) {
        callback(null, op.v + 1);
        return;
      }
      // If we don't have ops, use the version from the doc snapshot, or
      // default to 0 if there is no record of the doc
      self.getCollection(cName, function(err, collection) {
        if (err) return callback(err);
        var docQuery = {_id: docName};
        var docProjection = {_id: 0, _v: 1};
        collection.findOne(docQuery, docProjection, function(err, doc) {
          if (err) return callback(err);
          callback(null, doc ? doc._v : 0);
        });
      });
    });
  });
};

LiveDbMongo.prototype.getOps = function(cName, docName, start, end, callback) {
  this.getOpCollection(cName, function(err, opCollection) {
    if (err) return callback(err);
    var query = {
      $query: {
        name: docName,
        v: (end == null) ? {$gte: start} : {$gte: start, $lt: end}
      },
      $orderby: {v: 1}
    };
    // Exclude the `_id` & `name` fields, which are only for use internal to
    // livedb-mongo. Also exclude the `m` field, which can be used to store
    // metadata on ops for tracking purposes
    var projection = {
      _id: 0,
      name: 0,
      m: 0
    };
    opCollection.find(query, projection).toArray(callback);
  });
};


// **** Query methods

LiveDbMongo.prototype._query = function(collection, inputQuery, mongoProjection, callback) {
  var query = normalizeQuery(inputQuery);
  var err = this.checkQuery(query);
  if (err) return callback(err);

  if (query.$count) {
    collection.count(query.$query || {}, function(err, extra) {
      if (err) return callback(err);
      callback(null, [], extra);
    });
    return;
  }

  if (query.$distinct) {
    collection.distinct(query.$field, query.$query || {}, function(err, extra) {
      if (err) return callback(err);
      callback(null, [], extra);
    });
    return;
  }

  if (query.$aggregate) {
    collection.aggregate(query.$aggregate, function(err, extra) {
      if (err) return callback(err);
      callback(null, [], extra);
    });
    return;
  }

  if (query.$mapReduce) {
    var mapReduceOptions = {
      query: query.$query || {},
      out: {inline: 1},
      scope: query.$scope || {}
    };
    collection.mapReduce(query.$map, query.$reduce, mapReduceOptions, function(err, extra) {
      if (err) return callback(err);
      callback(null, [], extra);
    });
    return;
  }

  collection.find(query, mongoProjection, query.$findOptions).toArray(callback);
};

LiveDbMongo.prototype.query = function(cName, inputQuery, projection, options, callback) {
  var self = this;
  this.getCollection(cName, function(err, collection) {
    if (err) return callback(err);
    var mongoProjection = getMongoProjection(projection);
    self._query(collection, inputQuery, mongoProjection, function(err, results, extra) {
      if (err) return callback(err);
      callback(null, results.map(castToSnapshot), extra);
    });
  });
};

LiveDbMongo.prototype.queryPoll = function(cName, inputQuery, options, callback) {
  var self = this;
  this.getCollectionPoll(cName, function(err, collection) {
    if (err) return callback(err);
    var mongoProjection = {_id: 1};
    self._query(collection, inputQuery, mongoProjection, function(err, results, extra) {
      if (err) return callback(err);
      var docNames = [];
      for (var i = 0; i < results.length; i++) {
        docNames.push(results[i]._id);
      }
      callback(null, docNames, extra);
    });
  });
};

LiveDbMongo.prototype.queryPollDoc = function(cName, docName, inputQuery, options, callback) {
  var self = this;
  this.getCollectionPoll(cName, function(err, collection) {
    if (err) return callback(err);

    var query = normalizeQuery(inputQuery);
    var err = self.checkQuery(query);
    if (err) return callback(err);

    // Run the query against a particular mongo document by adding an _id filter
    var queryId = query.$query._id;
    if (queryId && typeof queryId === 'object') {
      // Check if the query contains the id directly in the common pattern of
      // a query for a specific list of ids, such as {_id: {$in: [1, 2, 3]}}
      if (Array.isArray(queryId.$in) && Object.keys(queryId).length === 1) {
        if (queryId.$in.indexOf(docName) === -1) {
          // If the id isn't in the list of ids, then there is no way this
          // can be a match
          return callback();
        } else {
          // If the id is in the list, then it is equivalent to restrict to our
          // particular id and override the current value
          queryId.$query._id = docName;
        }
      } else {
        delete query.$query._id;
        query.$query.$and = (query.$query.$and) ?
          query.$query.$and.concat({_id: docName}, {_id: queryId}) :
          [{_id: docName}, {_id: queryId}];
      }
    } else if (queryId && queryId !== docName) {
      // If queryId is a primative value such as a string or number and it
      // isn't equal to the docName, then there is no way this can be a match
      return callback();
    } else {
      // Restrict the query to this particular document
      query.$query._id = docName;
    }

    collection.findOne(query, {_id: 1}, function(err, doc) {
      callback(err, !!doc);
    });
  });
};


// **** Polling optimization

// Does the query need to be rerun against the database with every edit?
LiveDbMongo.prototype.queryNeedsPollMode = function(index, query) {
  return query.hasOwnProperty('$orderby') ||
    query.hasOwnProperty('$limit') ||
    query.hasOwnProperty('$skip') ||
    query.hasOwnProperty('$count');
};

// Tell livedb not to poll when it sees ops that can't change the query results
// because they are on unrelated fields
LiveDbMongo.prototype.queryShouldPoll = function(collection, docName, opData, index, query) {
  // Livedb is in charge of doing the validation of ops, so at this point we
  // should be able to assume that the op is structured validly
  if (opData.create || opData.del) return true;
  if (!opData.op) return false;
  var fields = getFields(query);
  return opContainsAnyField(opData.op, fields);
};

function getFields(query) {
  var fields = {};
  getInnerFields(query.$query, fields);
  getInnerFields(query.$orderby, fields);
  getInnerFields(query, fields);
  return fields;
}

function getInnerFields(params, fields) {
  if (!params) return;
  for (var key in params) {
    var value = params[key];
    if (key === '$or' || key === '$and') {
      for (var i = 0; i < value.length; i++) {
        var item = value[i];
        getInnerFields(item, fields);
      }
    } else if (key[0] !== '$') {
      var property = key.split('.')[0];
      fields[property] = true;
    }
  }
}

function opContainsAnyField(op, fields) {
  for (var i = 0; i < op.length; i++) {
    var component = op[i];
    if (component.p.length === 0) {
      return true;
    } else if (fields[component.p[0]]) {
      return true;
    }
  }
  return false;
}


// Utility methods

// Return error string on error. Query should already be normalized with
// normalizeQuery below.
LiveDbMongo.prototype.checkQuery = function(query) {
  if (!this.allowJSQueries) {
    if (query.$query.$where != null)
      return "$where queries disabled";
    if (query.$mapReduce != null)
      return "$mapReduce queries disabled";
  }

  if (!this.allowAggregateQueries && query.$aggregate)
    return "$aggregate queries disabled";
};

function normalizeQuery(inputQuery) {
  // Box queries inside of a $query and clone so that we know where to look
  // for selctors and can modify them without affecting the original object
  var query;
  if (inputQuery.$query) {
    query = shallowClone(inputQuery);
    query.$query = shallowClone(query.$query);
  } else {
    query = {$query: {}};
    for (var key in inputQuery) {
      if (metaOperators[key]) {
        query[key] = inputQuery[key];
      } else if (cursorOperators[key]) {
        var findOptions = query.$findOptions || (query.$findOptions = {});
        findOptions[cursorOperators[key]] = inputQuery[key];
      } else {
        query.$query[key] = inputQuery[key];
      }
    }
  }
  return query;
}

function castToDoc(docName, data) {
  var doc = (
    typeof data.data === 'object' &&
    data.data !== null &&
    !Array.isArray(data.data)
  ) ?
    shallowClone(data.data) :
    {_data: (data.data === undefined) ? null : data.data};
  doc._id = docName;
  doc._type = data.type;
  doc._v = data.v;
  doc._m = data.m;
  return doc;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var docName = doc._id;
  var type = doc._type;
  var data = doc._data;
  var v = doc._v;
  if (data === undefined) {
    data = shallowClone(doc);
    delete data._id;
    delete data._type;
    delete data._v;
    return new MongoSnapshot(docName, type, data, v);
  }
  return new MongoSnapshot(docName, type, data, v);
}
function MongoSnapshot(docName, type, data, v) {
  this.docName = docName;
  this.type = type;
  this.data = data;
  this.v = v;
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}

// The fields property is already pretty perfect for mongo. This will only work for JSON documents.
function getMongoProjection(projection) {
  var fields = projection && projection.fields;
  // When there is no projection specified, still exclude returning the metadata
  // that is added to a doc for querying or auditing
  if (!fields) return {_m: 0};
  var out = {};
  for (var key in fields) {
    out[key] = 1;
  }
  out._type = 1;
  out._v = 1;
  return out;
}
