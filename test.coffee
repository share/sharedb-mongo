# Mocha test using livedb's snapshot tests
mongodb = require 'mongodb'
LiveDbMongo = require './index'
assert = require 'assert'

# Clear mongo
clear = (callback) ->
  mongodb.connect 'mongodb://localhost:27017/test', (err, mongo) ->
    throw err if err
    mongo.dropCollection 'testcollection', ->
      mongo.dropCollection 'testcollection_ops', ->
        mongo.close callback

create = (callback) ->
  clear ->
    callback LiveDbMongo('mongodb://localhost:27017/test')

describe 'mongo', ->
  afterEach clear

  describe 'raw', ->
    beforeEach (done) ->
      mongodb.connect 'mongodb://localhost:27017/test', (err, db) =>
        @mongo = db
        create (@db) => done()

    afterEach (done) ->
      @mongo.close done

    it 'adds an index for ops', (done) -> create (db) =>
      db.writeOp 'testcollection', 'foo', {v:0, create:{type:'json0'}}, (err) =>
        @mongo.collection('testcollection_ops').indexInformation (err, indexes) ->
          throw err if err

          # We should find an index with [[ 'name', 1 ], [ 'v', 1 ]]
          for name, idx of indexes
            if JSON.stringify(idx) is '[["name",1],["v",1]]'
              return done()

          throw Error "Could not find index in ops db - #{JSON.stringify(indexes)}"

    it 'does not allow editing the system collection', (done) ->
      @db.writeSnapshot 'system', 'test', {type:'json0', v:5, m:{}, data:{x:5}}, (err) =>
        assert.ok err
        @db.getSnapshot 'system', 'test', null, (err, data) ->
          assert.ok err
          assert.equal data, null
          done()

    it 'defaults to the version of the document if there are no ops', (done) ->
      @db.writeSnapshot 'testcollection', 'versiontest', {type: 'json0', v: 3, data:{x:5}}, (err) =>
        throw Error err if err
        @db.getVersion 'testcollection', 'versiontest', (err, v) =>
          throw Error err if err
          assert.equal v, 3
          done()


    describe 'query', ->
      it 'returns data in the collection', (done) ->
        snapshot = {type:'json0', v:5, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.query 'testcollection', {x:5}, null, null, (err, results) ->
            throw Error err if err
            delete results[0].docName
            assert.deepEqual results, [snapshot]
            done()

      it 'returns nothing when there is no data', (done) ->
        @db.query 'testcollection', {x:5}, null, null, (err, results) ->
          throw Error err if err
          assert.deepEqual results, []
          done()

      it 'does not allow $where queries', (done) ->
        @db.query 'testcollection', {$where:"true"}, null, null, (err, results) ->
          assert.ok err
          assert.equal results, null
          done()

      it '$distinct should perform distinct operation', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{x:1, y:1}},
          {type:'json0', v:5, m:{}, data:{x:2, y:2}},
          {type:'json0', v:5, m:{}, data:{x:3, y:2}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              query = {$distinct: true, $field: 'y', $query: {}}
              @db.query 'testcollection', query, null, null, (err, results, extra) ->
                throw Error err if err
                assert.deepEqual extra, [1,2]
                done()

      it '$aggregate should perform aggregate command', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{x:1, y:1}},
          {type:'json0', v:5, m:{}, data:{x:2, y:2}},
          {type:'json0', v:5, m:{}, data:{x:3, y:2}}
        ]
        @db.allowAggregateQueries = true

        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              query = {$aggregate: [
                {$group: {_id: '$y', count: {$sum: 1}}}
                {$sort: {count: 1}}
              ]}
              @db.query 'testcollection', query, null, null, (err, results, extra) ->
                throw Error err if err
                assert.deepEqual extra, [{_id: 1, count: 1}, {_id: 2, count: 2}]
                done()

      it 'does not let you run $aggregate queries without options.allowAggregateQueries', (done) ->
        query = {$aggregate: [
          {$group: {_id: '$y', count: {$sum: 1}}}
          {$sort: {count: 1}}
        ]}
        @db.query 'testcollection', query, null, null, (err, results) ->
          assert.ok err
          done()

      it 'does not allow $mapReduce queries by default', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              query =
                $mapReduce: true,
                $map: ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              @db.query 'testcollection', query, null, null, (err, results) ->
                assert.ok err
                assert.equal results, null
                done()

      it '$mapReduce queries should work when allowJavaScriptQuery == true', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.allowJSQueries = true

        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              query =
                $mapReduce: true,
                $map: ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              @db.query 'testcollection', query, null, null, (err, results, extra) ->
                throw Error err if err
                assert.deepEqual extra, [{_id: 'a', value: 12}, {_id: 'b', value: 15}]
                done()

    describe 'query with projection', ->
      it 'returns only projected fields', (done) ->
        @db.writeSnapshot 'testcollection', 'test', {type:'json0', v:5, m:{}, data:{x:5, y:6}}, (err) =>
          @db.query 'testcollection', {x:5}, {y:true}, null, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, data:{y:6}, docName:'test'}]
            done()

      it 'returns no data for matching documents if fields is empty', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.query 'testcollection', {x:5}, {}, null, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, data:{}, docName:'test'}]
            done()

    describe 'queryPollDoc', ->
      it 'returns false when the document does not exist', (done) ->
        @db.queryPollDoc 'testcollection', 'doesnotexist', {}, null, (err, result) ->
          throw Error err if err
          assert.equal result, false
          done()

      it 'returns true when the document matches', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.queryPollDoc 'testcollection', 'test', {x:5}, null, (err, result) ->
            throw Error err if err
            assert.equal result, true
            done()

      it 'returns false when the document does not match', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.queryPollDoc 'testcollection', 'test', {x:6}, null, (err, result) ->
            throw Error err if err
            assert.equal result, false
            done()

      it 'does not allow $where queries', (done) ->
        @db.queryPollDoc 'testcollection', 'somedoc', {$where:"true"}, null, (err) ->
          assert.ok err
          done()

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
