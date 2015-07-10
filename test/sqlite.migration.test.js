var assert = require('assert');
var async = require('async');
var should = require('should');

require('mocha');
require('./init');

var Schema = require('loopback-datasource-juggler').Schema;

var UserData;
var db;

/* global describe, before, it, getDataSource */
/* eslint max-nested-callbacks:0 */
describe('migrations', function() {

  before(setup);

  it('should run migration', function(done) {
    db.automigrate(function() {
      done();
    });
  });

  it('UserData should have correct columns', function(done) {
    getFields('UserData', function(err, fields) {
      should.not.exists(err);
      var expected = {
        bio: {
          name: 'bio',
          type: 'TEXT',
          notnull: 0,
          pk: 0,
          dflt_value: null,
        },
        birthdate: {
          name: 'birthdate',
          type: 'INTEGER',
          notnull: 0,
          pk: 0,
          dflt_value: null,
        },
        createdbyadmin: {
          name: 'createdbyadmin',
          type: 'INTEGER',
          notnull: 0,
          pk: 0,
          dflt_value: null,
        },
        email: {
          name: 'email',
          type: 'TEXT',
          notnull: 1,
          pk: 0,
          dflt_value: null,
        },
        id: {
          name: 'id',
          type: 'INTEGER',
          notnull: 1,
          pk: 1,
          dflt_value: null,
        },
        name: {
          name: 'name',
          type: 'TEXT',
          notnull: 0,
          pk: 0,
          dflt_value: null,
        },
        pendingperiod: {
          name: 'pendingperiod',
          type: 'REAL',
          notnull: 0,
          pk: 0,
          dflt_value: null,
        },
      };

      expected.should.be.eql(fields);
      done();
    });
  });

  it('UserData should have correct indexes', function(done) {
    var expected = [{
      name: 'index0',
      table: 'userdata',
      unique: 0,
      partial: 0,
      columns: ['email', 'createdbyadmin']
    }, {
      name: 'userdata_email',
      table: 'userdata',
      unique: 0,
      partial: 0,
      columns: ['email']
    }];

    getIndexes('UserData', function(err, fields) {
      should.not.exists(err);
      expected.should.be.eql(fields);
      done();
    });
  });

  it('should autoupdate', function(done) {
    function userExists(cb) {
      query('SELECT * FROM UserData', function(err, res) {
        cb(!err && res[0].email === 'test@example.com');
      });
    }

    UserData.create({email: 'test@example.com'}, function(err) {
      assert.ok(!err, 'Could not create user: ' + err);
      userExists(function(yep) {
        assert.ok(yep, 'User does not exist');
      });
      UserData.defineProperty('email', {type: String});
      UserData.defineProperty('name', {
        type: String,
        dataType: 'char',
        limit: 50
      });
      UserData.defineProperty('newProperty', {
        type: Number,
        unsigned: true,
        dataType: 'bigInt'
      });
      db.autoupdate(function(err) {
        assert.ok(!err, 'Should not error');
        getFields('UserData', function(err, fields) {
          assert.ok(!err, 'Should not error');

          // change nullable for email
          assert.equal(fields.email.notnull, 0, 'Email does not allow null');
          // change type of name
          assert.equal(fields.name.type, 'TEXT', 'Name is not TEXT');
          // add new column
          assert.ok(fields.newproperty, 'New column was not added');
          if (fields.newProperty) {
            assert.equal(fields.newproperty.type, 'REAL',
              'New column type is not REAL');
          }
          userExists(function(yep) {
            assert.ok(yep, 'User does not exist');
            done();
          });
        });
      });
    });
  });

  it('should report errors for automigrate', function() {
    db.automigrate('XYZ', function(err) {
      assert(err);
    });
  });

  it('should report errors for autoupdate', function() {
    db.autoupdate('XYZ', function(err) {
      assert(err);
    });
  });

  it('should disconnect when done', function(done) {
    db.disconnect();
    done();
  });

});

function setup(done) {
  db = getDataSource();

  UserData = db.define('UserData', {
    email: {type: String, null: false, index: true},
    name: String,
    bio: Schema.Text,
    birthDate: Date,
    pendingPeriod: Number,
    createdByAdmin: Boolean,
  }, {
    indexes: {
      index0: {
        columns: 'email, createdByAdmin'
      }
    }
  });

  db.define('StringData', {
    idString: {type: String, id: true},
    smallString: {type: String, null: false, index: true,
      dataType: 'char', limit: 127},
    mediumString: {type: String, null: false, dataType: 'varchar', limit: 255},
    tinyText: {type: String, dataType: 'tinyText'},
    giantJSON: {type: Schema.JSON, dataType: 'longText'},
    text: {type: Schema.Text, dataType: 'varchar', limit: 1024}
  });

  db.define('NumberData', {
    number: {type: Number, null: false, index: true, unsigned: true,
      dataType: 'decimal', precision: 10, scale: 3},
    tinyInt: {type: Number, dataType: 'tinyInt', display: 2},
    mediumInt: {type: Number, dataType: 'mediumInt', unsigned: true,
      required: true},
    floater: {type: Number, dataType: 'double', precision: 14, scale: 6}
  });

  db.define('DateData', {
    dateTime: {type: Date, dataType: 'datetime'},
    timestamp: {type: Date, dataType: 'timestamp'}
  });
  done();
}

function query(sql, cb) {
  db.adapter.execute(sql, cb);
}

function getFields(model, cb) {
  query('PRAGMA table_info(' + model.toLowerCase() + ')', function(err, res) {
    if (err) {
      cb(err);
    } else {
      var fields = {};
      res.forEach(function(field) {
        delete field.cid;
        fields[field.name] = field;
      });
      cb(err, fields);
    }
  });
}

function getIndexes(model, cb) {
  query('PRAGMA index_list(' + model.toLowerCase() + ')', function(err, res) {
    if (err) {
      console.log(err);
      cb(err);
    } else {
      async.map(res, function(idx, cb) {
        query('PRAGMA index_info(' + idx.name + ')', function(err, res) {
          if (err) return cb(err);
          var indexInfo = {
            name: idx.name,
            table: model.toLowerCase(),
            unique: idx.unique,
            partial: idx.partial,
            columns: res.map(function(r) {
              return r.name;
            })
          };
          cb(null, indexInfo);
        });
      }, cb);
    }
  });
}
