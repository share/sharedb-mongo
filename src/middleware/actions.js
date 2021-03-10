module.exports = {
  // Triggers before the call to write a new document is made
  beforeCreate: 'beforeCreate',
  // Triggers before the call to replace a document is made
  beforeOverwrite: 'beforeOverwrite',
  // Triggers directly before the call to issue a query for snapshots
  // Applies for both a single lookup by ID and bulk lookups by a list of IDs
  beforeSnapshotLookup: 'beforeSnapshotLookup'
};
