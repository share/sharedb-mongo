## v1.0-beta

### Bugfixes

* Fix `skipPoll` for queries with `$not` or `$nor`

* Support Mongo 3.2

### Breaking changes

* Add options argument to all public database adapter methods that read
  or write from snapshots or ops.

* DB methods that get snapshots or ops no longer return metadata unless
  `{metadata: true}` option is passed.

* Deprecate `{new ShareDb({mongo: (mongo connection)})`. Instead, pass
  a callback in the `mongo` property.

* Change query format -- deprecate `$query`, support all Mongo methods
  as `$`-prefixed properties and change meaning of some meta operators.
  See the
  [query docs](https://github.com/share/sharedb-mongo#queries))
  for more details.

* Deprecate `$orderby` in favor of `$sort`

### Non-breaking changes

* Don't add {_type: {$ne: null}} in Mongo queries unless necessary

* Upgrade to Mongo driver 2.x

## v0.8.7

Beginning of changelog.
