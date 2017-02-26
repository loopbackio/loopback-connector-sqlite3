// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var DataSource = require('loopback-datasource-juggler').DataSource;
var path = require('path');
var os = require('os');
console.log('tmpdir',os.tmpdir())
var config = require('rc')('loopback', {
  test: {
    sqlite: {
      file: path.join(os.tmpdir(), 'test-' + Date.now().toString() + '.db')
    }
  }
}).test.sqlite;

global.getDataSource = global.getSchema = function() {
  var db = new DataSource(require('../'), config);
  db.log = function(a) {
    console.log(a);
  };
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};
