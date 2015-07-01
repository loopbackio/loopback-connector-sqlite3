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

      // XXX(KR): SQLite3 currently does not enforce FK checks but in the future
      // it might. Will need to disable/check & re-enable foreign key checks

      debug('autoupdate %j', self._models[model]);
      self.beginTransaction('EXCLUSIVE', function(err, connection) {
        if (err) return cb(err);
        var txOptions = {
          transation: {
            connection: connection,
            connector: self,
          }
        };
        var oldName = escape(model);
        var newName = escape(model + '_old');

        async.series([
          renameOldTable,
          self.createTable.bind(self, model, txOptions),
          copyRecords,
          dropOldTable,
          self.commit.bind(self, connection)
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
      });
    }, cb);

    function escape(name) {
      return self.escapeName(name.toLowerCase());
    }
  };
}
