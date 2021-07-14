var version = process.env._SHAREDB_MONGODB_DRIVER || 'mongodb';
var mongodb = require(version);
module.exports = mongodb;
