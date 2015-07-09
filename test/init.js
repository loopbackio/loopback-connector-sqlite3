var DataSource = require('loopback-datasource-juggler').DataSource;
var path = require('path');
var os = require('os');

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
