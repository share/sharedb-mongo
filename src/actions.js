module.exports = {
  // Triggers before the call to replace a document is made to Mongo
  beforeEdit: 'beforeEdit',
  // Triggers directly before the call to issue a query to Mongo for snapshots by ID
  beforeSnapshotLookup: 'beforeSnapshotLookup'
};
