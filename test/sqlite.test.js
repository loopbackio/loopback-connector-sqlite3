// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

require('loopback-datasource-juggler/test/common.batch.js');
require('loopback-datasource-juggler/test/include.test.js');
require('./init');

var should = require('should');

var Post;
var ModelWithPKString;
var db;

/*global describe, before, it, getDataSource*/
describe('sqlite3 connector', function() {

  before(function() {
    db = getDataSource();

    Post = db.define('PostWithBoolean', {
      title: {type: String, length: 255, index: true},
      content: {type: String},
      loc: 'GeoPoint',
      approved: Boolean
    });

    ModelWithPKString = db.define('ModelWithPKString', {
      id: {type: String, id: true},
      content: {type: String},
    });
  });

  it('should run migration', function(done) {
    db.automigrate(['PostWithBoolean', 'ModelWithPKString'], function(err) {
      done(err);
    });
  });

  var post;
  it('should support boolean types with true value', function(done) {
    Post.create({title: 'T1', content: 'C1', approved: true}, function(err, p) {
      should.not.exists(err);
      post = p;
      Post.findById(p.id, function(err, p) {
        should.not.exists(err);
        p.should.have.property('approved', true);
        done();
      });
    });
  });

  it('should support updating boolean types with false value', function(done) {
    Post.update({id: post.id}, {approved: false}, function(err) {
      should.not.exists(err);
      Post.findById(post.id, function(err, p) {
        should.not.exists(err);
        p.should.have.property('approved', false);
        done();
      });
    });
  });


  it('should support boolean types with false value', function(done) {
    Post.create({title: 'T2', content: 'C2', approved: false},
      function(err, p) {
        should.not.exists(err);
        post = p;
        Post.findById(p.id, function(err, p) {
          should.not.exists(err);
          p.should.have.property('approved', false);
          done();
        });
      }
    );
  });

  it('should return the model instance for upsert', function(done) {
    Post.upsert({id: post.id, title: 'T2_new', content: 'C2_new',
      approved: true}, function(err, p) {
      should.not.exists(err);
      p.should.have.property('id', post.id);
      p.should.have.property('title', 'T2_new');
      p.should.have.property('content', 'C2_new');
      p.should.have.property('approved', true);
      done();
    });
  });

  it('should return the model instance for upsert when id is not present',
    function(done) {
      Post.upsert({title: 'T2_new', content: 'C2_new', approved: true},
        function(err, p) {
          should.not.exists(err);
          p.should.have.property('id');
          p.should.have.property('title', 'T2_new');
          p.should.have.property('content', 'C2_new');
          p.should.have.property('approved', true);
          done();
        });
    });

  it('should escape number values to defect SQL injection in findById',
    function(done) {
      Post.findById('(SELECT 1+1)', function(err) {
        // SQLite3 doesnt error on invalid type
        should.not.exists(err);
        done();
      });
    });

  it('should escape number values to defect SQL injection in find',
    function(done) {
      Post.find({where: {id: '(SELECT 1+1)'}}, function(err) {
        // SQLite3 doesnt error on invalid type
        should.not.exists(err);
        done();
      });
    });

  it('should escape number values to defect SQL injection in find with gt',
    function(done) {
      Post.find({where: {id: {gt: '(SELECT 1+1)'}}}, function(err) {
        // SQLite3 doesnt error on invalid type
        should.not.exists(err);
        done();
      });
    });

  it('should escape number values to defect SQL injection in find',
    function(done) {
      Post.find({limit: '(SELECT 1+1)'}, function(err) {
        should.exists(err);
        done();
      });
    });

  it('should escape number values to defect SQL injection in find with inq',
    function(done) {
      Post.find({where: {id: {inq: ['(SELECT 1+1)']}}}, function(err) {
        // SQLite3 doesnt error on invalid type
        should.not.exists(err);
        done();
      });
    });

  it('should return the id inserted when primary key is a string type',
    function(done) {
      ModelWithPKString.create({id: 'PK_ID_TEST', content: 'content_TEST'},
        function(err, p) {
          should.not.exists(err);
          p.should.have.property('id', 'PK_ID_TEST');
          p.should.have.property('content', 'content_TEST');
          done();
        });
    });

  it('should return 0 documents when filtering with non existing field',
    function(done) {
      Post.count({nonexistingfield: '__TEST__'},
        function(err, count) {
          should.not.exists(err);
          count.should.equal(0);
          done();
        });
    });
});

// FIXME: The following test cases are to be reactivated for PostgreSQL
/*


 it('should support GeoPoint types', function(done) {
 var GeoPoint = juggler.ModelBuilder.schemaTypes.geopoint;
 var loc = new GeoPoint({lng: 10, lat: 20});
 Post.create({title: 'T1', content: 'C1', loc: loc}, function(err, p) {
 should.not.exists(err);
 Post.findById(p.id, function(err, p) {
 should.not.exists(err);
 p.loc.lng.should.be.eql(10);
 p.loc.lat.should.be.eql(20);
 done();
 });
 });
 });

 test.it('should not generate malformed SQL for number columns set to empty ' +
  'string', function(test) {
 var Post = dataSource.define('posts', {
 title: { type: String }
 , userId: { type: Number }
 });
 var post = new Post({title:'no userId', userId:''});

 Post.destroyAll(function() {
 post.save(function(err, post) {
 var id = post.id
 Post.all({where:{title:'no userId'}}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 });

 test.it('all should support regex', function(test) {
 Post = dataSource.models.Post;

 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id
 Post.all({where:{title:/^PostgreSQL/}}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 });

 test.it('all should support arbitrary expressions', function(test) {
 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id
 Post.all({where:{title:{ilike:'postgres%'}}}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 })

 test.it('all should support like operator ', function(test) {
 Post = dataSource.models.Post;
 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id
 Post.all({where:{title:{like:'%Test%'}}}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 });

 test.it('all should support \'not like\' operator ', function(test) {
 Post = dataSource.models.Post;
 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id
 Post.all({where:{title:{nlike:'%Test%'}}}, function(err, post) {
 test.ok(!err);
 test.ok(post.length===0);
 test.done();
 });
 });
 });
 });

 test.it('all should support arbitrary where clauses', function(test) {
 Post = dataSource.models.Post;
 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id;
 Post.all({where:"title = 'PostgreSQL Test Title'"}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 });

 test.it('all should support arbitrary parameterized where clauses',
 function(test) {
 Post = dataSource.models.Post;
 Post.destroyAll(function() {
 Post.create({title:'PostgreSQL Test Title'}, function(err, post) {
 var id = post.id;
 Post.all({where:['title = ?', 'PostgreSQL Test Title']}, function(err, post) {
 test.ok(!err);
 test.ok(post[0].id == id);
 test.done();
 });
 });
 });
 });
 */
