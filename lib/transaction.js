// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var debug = require('debug')('loopback:connector:sqlite3:transaction');

module.exports = mixinTransaction;

/*!
 * @param {sqlite3} SQLite3 connector class
 */
function mixinTransaction(SQLite3) {

  /**
   * Begin a new transaction
   * @param {string} isolationLevel
   * @param {function} cb
   */
  SQLite3.prototype.beginTransaction = function(isolationLevel, cb) {
    if (typeof isolationLevel === 'function') {
      cb = isolationLevel;
      isolationLevel = 'DEFERRED';
    }

    debug('Begin a transaction with isolation level: %s', isolationLevel);
    this._getConnection(function(err, connection) {
      if (err) return cb(err);
      connection.run('BEGIN ' + isolationLevel + ' TRANSACTION',
        function(err) {
          if (err) return cb(err);
          connection.release = connection.close.bind(connection);
          cb(null, connection);
        });
    });
  };

  SQLite3.prototype.commit = function(connection, cb) {
    debug('Commit a transaction');
    var self = this;
    connection.run('COMMIT', function(err) {
      self.releaseConnection(connection, err);
      cb(err);
    });
  };

  SQLite3.prototype.rollback = function(connection, cb) {
    debug('Rollback a transaction');
    var self = this;
    connection.run('ROLLBACK', function(err) {
      //if there was a problem rolling back the query
      //something is seriously messed up.  Return the error
      //to the done function to close & remove this client from
      //the pool.  If you leave a client in the pool with an unaborted
      //transaction weird, hard to diagnose problems might happen.
      self.releaseConnection(connection, err);
      cb(err);
    });
  };

  SQLite3.prototype.releaseConnection = function(connection, err) {
    if (err) debug('Error while cleaning up connection: %s', err.message);
    connection.release();
  };
}
