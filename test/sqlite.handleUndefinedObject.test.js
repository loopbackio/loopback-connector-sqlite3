// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var should = require('should');
require('./init');

var Post, db;

/*global describe, before, it, getDataSource*/
describe('handle undefined object', function() {
  before(function() {
    db = getDataSource();

    Post = db.define('HandleUndefinedObject', {
      field1: {
        type: 'number'
      },
      field2: {
        type: 'object'
      }
    });
  });

  it('should run migration', function(done) {
    db.automigrate('HandleUndefinedObject', function() {
      done();
    });
  });

  it('should handle object field in fields list', function(done) {
    var tstPost = new Post();
    tstPost.field1 = 1;
    tstPost.field2 = {val: 2};
    tstPost.save(function(err, p) {
      should.not.exist(err);
      Post.findOne({where: {id: p.id}}, function(err, p) {
        should.not.exist(err);
        p.field1.should.equal(1);
        p.field2.should.deepEqual({val: 2});
        done();
      });
    });
  });

  it('should handle object field not in fields list', function(done) {
    var tstPost = new Post();
    tstPost.field1 = 1;
    tstPost.field2 = {val: 2};
    tstPost.save(function(err, p) {
      should.not.exist(err);
      Post.findOne({where: {id: p.id}, fields: ['field1']}, function(err, p) {
        should.not.exist(err);
        p.field1.should.be.equal(1);
        should.not.exist(p.field2);
        done();
      });
    });
  });

  // it('should insert default value and \'third\' field', function(done) {
  //   var tstPost = new Post();
  //   tstPost.third = 3;
  //   tstPost.save(function(err, p) {
  //     should.not.exist(err);
  //     Post.findOne({where: {id: p.id}}, function(err, p) {
  //       should.not.exist(err);
  //       p.defaultInt.should.be.equal(5);
  //       should.not.exist(p.first);
  //       should.not.exist(p.second);
  //       should.exist(p.third);
  //       p.third.should.be.equal(3);
  //     });
  //     done();
  //   });
  // });

});
