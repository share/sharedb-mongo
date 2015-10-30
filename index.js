var async = require('async');
var mongodb = require('mongodb');
var DB = require('sharedb').DB;

module.exports = ShareDbMongo;

function ShareDbMongo(mongo, options) {
  // use without new
  if (!(this instanceof ShareDbMongo)) {
    return new ShareDbMongo(mongo, options);
  }

  if (typeof mongo === 'object') {
    options = mongo;
    mongo = options.mongo;
  }
  if (!options) options = {};

  // pollDelay is a dodgy hack to work around race conditions replicating the
  // data out to the polling target secondaries. If a separate db is specified
  // for polling, it defaults to 300ms
  this.pollDelay = (options.pollDelay != null) ? options.pollDelay :
    (options.mongoPoll) ? 300 : 0;

  // By default, we create indexes on any ops collection that is used
  this.disableIndexCreation = options.disableIndexCreation || false;

  // The getOps() method depends on a collectionname_ops collection, and that
  // collection should have an index on the operations stored there. We could
  // ask people to make these indexes themselves, but by default the mongo
  // driver will do it automatically. This approach will leak memory relative
  // to the number of collections you have. This should be OK, as we are not
  // expecting thousands of mongo collections.

  // Map from collection name -> true for op collections we've ensureIndex'ed
  this.opIndexes = {};

  // Allow $while and $mapReduce queries. These queries let you run arbitrary
  // JS on the server. If users make these queries from the browser, there's
  // security issues.
  this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || false;

  // Aggregate queries are less dangerous, but you can use them to access any
  // data in the mongo database.
  this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries || false;

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

ShareDbMongo.prototype = Object.create(DB.prototype);

ShareDbMongo.prototype.projectsSnapshots = true;

ShareDbMongo.prototype.getCollection = function(collectionName, callback) {
  // Check the collection name
  var err = this.validateCollectionName(collectionName);
  if (err) return callback(err);
  // Gotcha: calls back sync if connected or async if not
  this.getDbs(function(err, mongo) {
    if (err) return callback(err);
    var collection = mongo.collection(collectionName);
    return callback(null, collection);
  });
};

ShareDbMongo.prototype._getCollectionPoll = function(collectionName, callback) {
  // Check the collection name
  var err = this.validateCollectionName(collectionName);
  if (err) return callback(err);
  // Gotcha: calls back sync if connected or async if not
  this.getDbs(function(err, mongo, mongoPoll) {
    if (err) return callback(err);
    var collection = (mongoPoll || mongo).collection(collectionName);
    return callback(null, collection);
  });
};

ShareDbMongo.prototype.getCollectionPoll = function(collectionName, callback) {
  if (this.pollDelay) {
    var self = this;
    setTimeout(function() {
      self._getCollectionPoll(collectionName, callback);
    }, this.pollDelay);
    return;
  }
  this._getCollectionPoll(collectionName, callback);
};

ShareDbMongo.prototype.getDbs = function(callback) {
  if (this.closed) {
    var err = {code: 5101, message: 'Already closed'};
    return callback(err);
  }
  // We consider ouself ready to reply if this.mongo is defined and don't check
  // this.mongoPoll, since it is optional and is null by default. Thus, it's
  // important that these two properties are only set together synchronously
  if (this.mongo) return callback(null, this.mongo, this.mongoPoll);
  this.pendingConnect.push(callback);
};

ShareDbMongo.prototype._flushPendingConnect = function() {
  var pendingConnect = this.pendingConnect;
  this.pendingConnect = null;
  for (var i = 0; i < pendingConnect.length; i++) {
    pendingConnect[i](null, this.mongo, this.mongoPoll);
  }
};

ShareDbMongo.prototype._connect = function(mongo, options) {
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

ShareDbMongo.prototype.close = function(callback) {
  var self = this;
  this.getDbs(function(err, mongo, mongoPoll) {
    if (err) return callback && callback(err);
    self.closed = true;
    var closeCb = (mongoPoll) ?
      function(err) {
        if (err) return callback && callback(err);
        mongoPoll.close(callback);
      } :
      callback;
    mongo.close(closeCb);
  });
};


// **** Commit methods

ShareDbMongo.prototype.commit = function(collectionName, id, op, snapshot, callback) {
  var self = this;
  this._writeOp(collectionName, id, op, snapshot, function(err, result) {
    if (err) return callback(err);
    var opId = result.insertedId;
    self._writeSnapshot(collectionName, id, snapshot, opId, function(err, succeeded) {
      if (succeeded) return callback(err, succeeded);
      // Cleanup unsuccessful op if snapshot write failed. This is not
      // neccessary for data correctness, but it gets rid of clutter
      self._deleteOp(collectionName, opId, function(removeErr) {
        callback(err || removeErr, succeeded);
      });
    });
  });
};

ShareDbMongo.prototype._writeOp = function(collectionName, id, op, snapshot, callback) {
  if (typeof op.v !== 'number') {
    var err = {
      code: 4101,
      message: 'Invalid op version ' + collectionName + '.' + id + ' ' + op.v
    };
    return callback(err);
  }
  this.getOpCollection(collectionName, function(err, opCollection) {
    if (err) return callback(err);
    var doc = shallowClone(op);
    doc.d = id;
    doc.o = snapshot._opLink;
    opCollection.insertOne(doc, callback);
  });
};

ShareDbMongo.prototype._deleteOp = function(collectionName, opId, callback) {
  this.getOpCollection(collectionName, function(err, opCollection) {
    if (err) return callback(err);
    opCollection.deleteOne({_id: opId}, callback);
  });
};

ShareDbMongo.prototype._writeSnapshot = function(collectionName, id, snapshot, opLink, callback) {
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var doc = castToDoc(id, snapshot, opLink);
    if (doc._v === 1) {
      collection.insertOne(doc, function(err, result) {
        if (err) {
          // Return non-success instead of duplicate key error, since this is
          // expected to occur during simultaneous creates on the same id
          if (err.code === 11000) return callback(null, false);
          return callback(err);
        }
        callback(null, true);
      });
    } else {
      collection.replaceOne({_id: id, _v: doc._v - 1}, doc, function(err, result) {
        if (err) return callback(err);
        var succeeded = !!result.modifiedCount;
        callback(null, succeeded);
      });
    }
  });
};


// **** Snapshot methods

ShareDbMongo.prototype.getSnapshot = function(collectionName, id, fields, callback) {
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: id};
    var projection = getProjection(fields);
    collection.findOne(query, projection, function(err, doc) {
      if (err) return callback(err);
      var snapshot = (doc) ? castToSnapshot(doc) : new MongoSnapshot(id, 0, null, null);
      callback(null, snapshot);
    });
  });
};

ShareDbMongo.prototype.getSnapshotBulk = function(collectionName, ids, fields, callback) {
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: {$in: ids}};
    var projection = getProjection(fields);
    collection.find(query, projection).toArray(function(err, docs) {
      if (err) return callback(err);
      var snapshotMap = {};
      for (var i = 0; i < docs.length; i++) {
        var snapshot = castToSnapshot(docs[i]);
        snapshotMap[snapshot.id] = snapshot;
      }
      var uncreated = [];
      for (var i = 0; i < ids.length; i++) {
        var id = ids[i];
        if (snapshotMap[id]) continue;
        snapshotMap[id] = new MongoSnapshot(id, 0, null, null);
      }
      callback(null, snapshotMap);
    });
  });
};


// **** Oplog methods

// Overwrite me if you want to change this behaviour.
ShareDbMongo.prototype.getOplogCollectionName = function(collectionName) {
  return 'ops_' + collectionName;
};

ShareDbMongo.prototype.validateCollectionName = function(collectionName) {
  if (
    collectionName === 'system' || (
      collectionName[0] === 'o' &&
      collectionName[1] === 'p' &&
      collectionName[2] === 's' &&
      collectionName[3] === '_'
    )
  ) {
    return {code: 4102, message: 'Invalid collection name ' + collectionName};
  }
};

// Get and return the op collection from mongo, ensuring it has the op index.
ShareDbMongo.prototype.getOpCollection = function(collectionName, callback) {
  var self = this;
  this.getDbs(function(err, mongo) {
    if (err) return callback(err);
    var name = self.getOplogCollectionName(collectionName);
    var collection = mongo.collection(name);
    // Given the potential problems with creating indexes on the fly, it might
    // be preferrable to disable automatic creation
    if (self.disableIndexCreation) {
      return callback(null, collection);
    }
    if (self.opIndexes[collectionName]) {
      return callback(null, collection);
    }
    // WARNING: Creating indexes automatically like this is quite dangerous in
    // production if we are starting with a lot of data and no indexes
    // already. If new indexes were added or definition of these indexes were
    // changed, users upgrading this module could unsuspectingly lock up their
    // databases. If indexes are created as the first ops are added to a
    // collection this won't be a problem, but this is a dangerous mechanism.
    // Perhaps we should only warn instead of creating the indexes, especially
    // when there is a lot of data in the collection.
    collection.createIndex({d: 1, v: 1}, {background: true}, function(err) {
      if (err) return callback(err);
      self.opIndexes[collectionName] = true;
      callback(null, collection);
    });
  });
};

ShareDbMongo.prototype.getOpsToSnapshot = function(collectionName, id, from, snapshot, callback) {
  if (snapshot._opLink == null) {
    var err = getSnapshotOpLinkErorr(collectionName, id);
    return callback(err);
  }
  this._getOps(collectionName, id, from, function(err, ops) {
    if (err) return callback(err);
    var filtered = getLinkedOps(ops, null, snapshot._opLink);
    var err = checkOpsFrom(collectionName, id, filtered, from);
    if (err) return callback(err);
    callback(null, filtered);
  });
};

ShareDbMongo.prototype.getOps = function(collectionName, id, from, to, callback) {
  var self = this;
  this._getSnapshotOpLink(collectionName, id, function(err, doc) {
    if (err) return callback(err);
    if (doc) {
      if (isCurrentVersion(doc, from)) {
        return callback(null, []);
      }
      var err = doc && checkDocHasOp(collectionName, id, doc);
      if (err) return callback(err);
    }
    self._getOps(collectionName, id, from, function(err, ops) {
      if (err) return callback(err);
      var filtered = filterOps(ops, doc, to);
      var err = checkOpsFrom(collectionName, id, filtered, from);
      if (err) return callback(err);
      callback(null, filtered);
    });
  });
};

ShareDbMongo.prototype.getOpsBulk = function(collectionName, fromMap, toMap, callback) {
  var self = this;
  var ids = Object.keys(fromMap);
  this._getSnapshotOpLinkBulk(collectionName, ids, function(err, docs) {
    if (err) return callback(err);
    var docMap = getDocMap(docs);
    // Add empty array for snapshot versions that are up to date and create
    // the query conditions for ops that we need to get
    var conditions = [];
    var opsMap = {};
    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      var doc = docMap[id];
      var from = fromMap[id];
      if (doc) {
        if (isCurrentVersion(doc, from)) {
          opsMap[id] = [];
          continue;
        }
        var err = checkDocHasOp(collectionName, id, doc);
        if (err) return callback(err);
      }
      conditions.push({
        d: id,
        v: {$gte: from}
      });
    }
    // Return right away if none of the snapshot versions are newer than the
    // requested versions
    if (!conditions.length) return callback(null, opsMap);
    // Otherwise, get all of the ops that are newer
    self._getOpsBulk(collectionName, conditions, function(err, opsBulk) {
      if (err) return callback(err);
      for (var i = 0; i < conditions.length; i++) {
        var id = conditions[i].d;
        var ops = opsBulk[id];
        var doc = docMap[id];
        var from = fromMap[id];
        var to = toMap && toMap[id];
        var filtered = filterOps(ops, doc, to);
        var err = checkOpsFrom(collectionName, id, filtered, from);
        if (err) return callback(err);
        opsMap[id] = filtered;
      }
      callback(null, opsMap);
    });
  });
};

function checkOpsFrom(collectionName, id, ops, from) {
  if (ops[0] && ops[0].v === from) return;
  return {
    code: 5103,
    message: 'Missing ops from requested version ' + collectionName + '.' + id + ' ' + from
  }
};

function getSnapshotOpLinkErorr(collectionName, id) {
  return {
    code: 5102,
    message: 'Snapshot missing last operation field "_o" ' + collectionName + '.' + id
  };
}

function checkDocHasOp(collectionName, id, doc) {
  if (doc._o) return;
  return getSnapshotOpLinkErorr(collectionName, id);
}

function isCurrentVersion(doc, version) {
  return doc._v === version;
}

function getDocMap(docs) {
  var docMap = {};
  for (var i = 0; i < docs.length; i++) {
    var doc = docs[i];
    docMap[doc._id] = doc;
  }
  return docMap;
}

function filterOps(ops, doc, to) {
  // Always return in the case of no ops found whether or not consistent with
  // the snapshot
  if (!ops) return [];
  if (!ops.length) return ops;
  if (!doc) {
    // There is no snapshot currently. We already returned if there are no
    // ops, so this could happen if:
    //   1. The doc was deleted
    //   2. The doc create op is written but not the doc snapshot
    //   3. Same as 3 for a recreate
    //   4. We are in an inconsistent state because of an error
    //
    // We treat the snapshot as the canonical version, so if the snapshot
    // doesn't exist, the doc should be considered deleted. Thus, a delete op
    // should be in the last version if no commits are inflight or second to
    // last version if commit(s) are inflight. Rather than trying to detect
    // ops inconsistent with a deleted state, we are simply returning ops from
    // the last delete. Inconsistent states will ultimately cause write
    // failures on attempt to commit.
    //
    // Different delete ops must be identical and must link back to the same
    // prior version in order to be inserted, so if there are multiple delete
    // ops at the same version, we can grab any of them for this method.
    // However, the _id of the delete op might not ultimately match the delete
    // op that gets maintained if two are written as a result of two
    // simultanous delete commits. Thus, the _id of the op should *not* be
    // assumed to be consistent in the future.
    var deleteOp = getLatestDeleteOp(ops);
    // Don't return any ops if we don't find a delete operation, which is the
    // correct thing to do if the doc was just created and the op has been
    // written but not the snapshot. Note that this will simply return no ops
    // if there are ops but the snapshot doesn't exist.
    if (!deleteOp) return [];
    return getLinkedOps(ops, to, deleteOp._id);
  }
  return getLinkedOps(ops, to, doc._o);
}

function getLatestDeleteOp(ops) {
  for (var i = ops.length; i--;) {
    var op = ops[i];
    if (op.del) return op;
  }
}

function getLinkedOps(ops, to, link) {
  var linkedOps = []
  for (var i = ops.length; i-- && link;) {
    var op = ops[i];
    if (link.equals ? !link.equals(op._id) : link !== op._id) continue;
    link = op.o;
    if (to == null || op.v < to) {
      delete op._id;
      delete op.o;
      linkedOps.unshift(op);
    }
  }
  return linkedOps;
}

ShareDbMongo.prototype._getOps = function(collectionName, id, from, callback) {
  this.getOpCollection(collectionName, function(err, opCollection) {
    if (err) return callback(err);
    var query = {
      $query: {
        d: id,
        v: {$gte: from}
      },
      $orderby: {v: 1}
    };
    // Exclude the `d` field, which is only for use internal to livedb-mongo.
    // Also exclude the `m` field, which can be used to store metadata on ops
    // for tracking purposes
    var projection = {d: 0, m: 0};
    opCollection.find(query, projection).toArray(callback);
  });
};

ShareDbMongo.prototype._getOpsBulk = function(collectionName, conditions, callback) {
  this.getOpCollection(collectionName, function(err, opCollection) {
    if (err) return callback(err);
    var query = {
      $query: {$or: conditions},
      $orderby: {d: 1, v: 1}
    };
    // Exclude the `m` field, which can be used to store metadata on ops for
    // tracking purposes
    var projection = {m: 0};
    opCollection.find(query, projection, function(err, cursor) {
      if (err) return callback(err);
      readOpsBulk(cursor, {}, null, null, callback);
    });
  });
};

function readOpsBulk(cursor, opsMap, id, ops, callback) {
  cursor.nextObject(function(err, op) {
    if (err) return callback(err);
    if (op == null) {
      if (id) opsMap[id] = ops;
      return callback(null, opsMap);
    }
    if (id !== op.d) {
      if (id) opsMap[id] = ops;
      id = op.d;
      ops = [op];
    } else {
      ops.push(op);
    }
    delete op.d;
    readOpsBulk(cursor, opsMap, id, ops, callback);
  });
}

ShareDbMongo.prototype._getSnapshotOpLink = function(collectionName, id, callback) {
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: id};
    var projection = {_id: 0, _o: 1, _v: 1};
    collection.findOne(query, projection, callback);
  });
};

ShareDbMongo.prototype._getSnapshotOpLinkBulk = function(collectionName, ids, callback) {
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var query = {_id: {$in: ids}};
    var projection = {_o: 1, _v: 1};
    collection.find(query, projection).toArray(callback);
  });
};


// **** Query methods

ShareDbMongo.prototype._query = function(collection, inputQuery, projection, callback) {
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

  collection.find(query, projection, query.$findOptions).toArray(callback);
};

ShareDbMongo.prototype.query = function(collectionName, inputQuery, fields, options, callback) {
  var self = this;
  this.getCollection(collectionName, function(err, collection) {
    if (err) return callback(err);
    var projection = getProjection(fields);
    self._query(collection, inputQuery, projection, function(err, results, extra) {
      if (err) return callback(err);
      var snapshots = [];
      for (var i = 0; i < results.length; i++) {
        var snapshot = castToSnapshot(results[i]);
        snapshots.push(snapshot);
      }
      callback(null, snapshots, extra);
    });
  });
};

ShareDbMongo.prototype.queryPoll = function(collectionName, inputQuery, options, callback) {
  var self = this;
  this.getCollectionPoll(collectionName, function(err, collection) {
    if (err) return callback(err);
    var projection = {_id: 1};
    self._query(collection, inputQuery, projection, function(err, results, extra) {
      if (err) return callback(err);
      var ids = [];
      for (var i = 0; i < results.length; i++) {
        ids.push(results[i]._id);
      }
      callback(null, ids, extra);
    });
  });
};

ShareDbMongo.prototype.queryPollDoc = function(collectionName, id, inputQuery, options, callback) {
  var self = this;
  this.getCollectionPoll(collectionName, function(err, collection) {
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
        if (queryId.$in.indexOf(id) === -1) {
          // If the id isn't in the list of ids, then there is no way this
          // can be a match
          return callback();
        } else {
          // If the id is in the list, then it is equivalent to restrict to our
          // particular id and override the current value
          queryId.$query._id = id;
        }
      } else {
        delete query.$query._id;
        query.$query.$and = (query.$query.$and) ?
          query.$query.$and.concat({_id: id}, {_id: queryId}) :
          [{_id: id}, {_id: queryId}];
      }
    } else if (queryId && queryId !== id) {
      // If queryId is a primative value such as a string or number and it
      // isn't equal to the id, then there is no way this can be a match
      return callback();
    } else {
      // Restrict the query to this particular document
      query.$query._id = id;
    }

    collection.findOne(query, {_id: 1}, function(err, doc) {
      callback(err, !!doc);
    });
  });
};


// **** Polling optimization

// Can we poll by checking the query limited to the particular doc only?
ShareDbMongo.prototype.canPollDoc = function(collectionName, query) {
  return !(
    query.hasOwnProperty('$orderby') ||
    query.hasOwnProperty('$limit') ||
    query.hasOwnProperty('$skip') ||
    query.hasOwnProperty('$count')
  );
};

// Return true to avoid polling if there is no possibility that an op could
// affect a query's results
ShareDbMongo.prototype.skipPoll = function(collectionName, id, op, query) {
  // Livedb is in charge of doing the validation of ops, so at this point we
  // should be able to assume that the op is structured validly
  if (op.create || op.del) return false;
  if (!op.op) return true;
  var fields = getFields(query);
  return !opContainsAnyField(op.op, fields);
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
ShareDbMongo.prototype.checkQuery = function(query) {
  if (!this.allowJSQueries) {
    if (query.$query.$where != null) {
      return {code: 4103, message: '$where queries disabled'};
    }
    if (query.$mapReduce != null) {
      return {code: 4104, message: '$mapReduce queries disabled'};
    }
  }

  if (!this.allowAggregateQueries && query.$aggregate) {
    return {code: 4105, message: '$aggregate queries disabled'};
  }
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
  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. Lack of a type indicates that a
  // snapshot is deleted, so don't return any documents with a null type
  if (!query.$query._type) query.$query._type = {$ne: null};
  return query;
}

function castToDoc(id, snapshot, opLink) {
  var doc = (
    typeof snapshot.data === 'object' &&
    snapshot.data !== null &&
    !Array.isArray(snapshot.data)
  ) ?
    shallowClone(snapshot.data) :
    {_data: snapshot.data};
  doc._id = id;
  doc._type = snapshot.type;
  doc._v = snapshot.v;
  doc._m = snapshot.m;
  doc._o = opLink;
  return doc;
}

function castToSnapshot(doc) {
  var id = doc._id;
  var version = doc._v;
  var type = doc._type;
  var data = doc._data;
  var meta = doc._m;
  var opLink = doc._o;
  if (doc.hasOwnProperty('_data')) {
    return new MongoSnapshot(id, version, type, data, meta, opLink);
  }
  var data = shallowClone(doc);
  delete data._id;
  delete data._v;
  delete data._type;
  delete data._m;
  delete data._o;
  return new MongoSnapshot(id, version, type, data, meta, opLink);
}
function MongoSnapshot(id, version, type, data, meta, opLink) {
  this.id = id;
  this.v = version;
  this.type = type;
  this.data = data;
  if (meta) this.m = meta;
  if (opLink) this._opLink = opLink;
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}

// Convert a simple map of fields that we want into a mongo projection. This
// depends on the data being stored at the top level of the document. It will
// only work properly for json documents--which are the only types for which
// we really want projections.
function getProjection(fields) {
  // Do not project when called by ShareDB submit
  if (fields === 'submit') return;
  // When there is no projection specified, still exclude returning the metadata
  // that is added to a doc for querying or auditing
  if (!fields) return {_m: 0, _o: 0};
  var projection = {};
  for (var key in fields) {
    projection[key] = 1;
  }
  projection._type = 1;
  projection._v = 1;
  return projection;
}

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
