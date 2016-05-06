// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var async = require('async');
var debug = require('debug')('loopback:connector:sqlite3:migration');
var fmt = require('util').format;

module.exports = mixinMigration;

/*!
 * @param {SQLite3} SQLite3 connector class
 */
function mixinMigration(SQLite3) {
  /**
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names.
   * If not present, apply to all models
   * @param {Function} [cb] The callback function
   */
  SQLite3.prototype.autoupdate = function(models, cb) {
    var self = this;

    if ((!cb) && (typeof models === 'function')) {
      cb = models;
      models = undefined;
    }

    async.eachSeries(models, function(model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function() {
          done(new Error('Model not found: ' + model));
        });
      }

      self.beginTransaction('EXCLUSIVE', function(err, connection) {
        if (err) return done(err);

        var txOptions = {
          transaction: {
            connection: connection,
            connector: self,
          }
        };

        debug('autoupdate %s', model);
        var stmt = fmt(
          'SELECT name FROM sqlite_master WHERE TYPE="table" AND name=%s',
          self.tableEscaped(model)
        );
        self.executeSQL(stmt, [], txOptions, function(err, rows) {
          if (err) return checkAndCommit(err);

          if (rows.length > 0) {
            debug('existing table found for %j', self._models[model]);
            return self._alterTable(model, txOptions, checkAndCommit);
          }
          debug('Creating new table for %j', self._models[model]);
          self.createTable(model, txOptions, checkAndCommit);
        });

        function checkAndCommit(err) {
          if (err) {
            debug('Error in transaction: %s', err.message);
            return self.rollback(connection, function() {
              done(err);
            });
          }
          self.commit(connection, done);
        }
      });
    }, cb);
  };

  SQLite3.prototype._alterTable = function(model, txOptions, done) {
    var self = this;

    // XXX(KR): SQLite3 currently does not enforce FK checks but in the future
    // it might. Will need to disable/check & re-enable foreign key checks
    var oldName = escape(model);
    var newName = escape(model + '_old');

    async.series([
      renameOldTable,
      self.createTable.bind(self, model, txOptions),
      copyRecords,
      dropOldTable,
    ], done);

    function renameOldTable(cb) {
      self.executeSQL(
        fmt('ALTER TABLE %s RENAME TO %s', oldName, newName),
        [], txOptions, cb
      );
    }

    function copyRecords(cb) {
      var properties = self.getModelDefinition(model).properties;
      properties = Object.keys(properties).map(function(name) {
        return escape(name);
      }).join(',');

      self.executeSQL(
        fmt('INSERT INTO %s SELECT %s FROM %s',
          oldName, properties, newName
        ), [], txOptions, cb
      );
    }

    function dropOldTable(cb) {
      self.executeSQL(fmt('DROP TABLE %s', newName), [], txOptions, cb);
    }

    function escape(name) {
      return self.escapeName(name.toLowerCase());
    }
  };
}
