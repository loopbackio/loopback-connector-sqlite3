var assert = require('assert');
var should = require('should');
require('./init');

var Post, db;

/*global describe, before, it, getDataSource*/
describe('database default field values', function() {
  before(function() {
    db = getDataSource();

    Post = db.define('PostWithDbDefaultValue', {
      created: {
        type: 'Date',
        sqlite3: {
          dbDefault: 'now'
        }
      },
      defaultInt: {
        type: 'Number',
        sqlite3: {
          dbDefault: '5'
        }
      },
      oneMore: {
        type: 'Number'
      }
    });

    db.define('PostWithInvalidDbDefaultValue', {
      created: {
        type: 'Date',
        sqlite3: {
          dbDefault: '\'5\''
        }
      }
    });
  });

  it('should run migration', function(done) {
    db.automigrate('PostWithDbDefaultValue', function() {
      done();
    });
  });

  it('should report inconsistent default values used', function(done) {
    db.automigrate('PostWithInvalidDbDefaultValue', function(err) {
      should.exists(err);
      done();
    });
  });

  it('should have \'now\' default value in SQL column definition',
    function(done) {
      var query = 'PRAGMA table_info(postwithdbdefaultvalue)';

      db.connector.executeSQL(query, function(err, results) {
        assert.equal(results[0].dflt_value,
          'CAST(STRFTIME(\'%s\', \'now\') AS INTEGER)*1000');
        done(err);
      });
    });

  it('should create a record with default value', function(done) {
    Post.create({oneMore: 3}, function(err) {
      should.not.exists(err);
      Post.findOne({where: {defaultInt: 5}}, function(err, p) {
        should.not.exists(err);
        should.exists(p);
        p.should.have.property('defaultInt', 5);
        done();
      });
    });
  });

  it('should create a record with custom value', function(done) {
    Post.create({oneMore: 2, defaultInt: 6}, function(err) {
      should.not.exists(err);
      Post.findOne({where: {defaultInt: 6}}, function(err, p) {
        should.not.exists(err);
        should.exists(p);
        p.should.have.property('defaultInt', 6);
        done();
      });
    });
  });
});
