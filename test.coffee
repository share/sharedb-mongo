# Mocha test using livedb's snapshot tests
mongoskin = require 'mongoskin'
liveDbMongo = require './lib'

require('livedb/test/snapshotdb') -> liveDbMongo 'localhost:27017/test?auto_reconnect', safe: false

