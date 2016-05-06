// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-sqlite3
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/*!
 * SQLite3 connector for LoopBack
 */
var SqlConnector = require('loopback-connector').SqlConnector;
var async = require('async');
var debug = require('debug')('loopback:connector:sqlite3');
var fmt = require('util').format;
var sqlite3 = require('sqlite3');
var util = require('util');
var moment = require('moment');

function InvalidParam(msg) {
  this.msg = msg;
}
/**
 *
 * Initialize the SQLite3 connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header SQLite3.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!sqlite3) {
    return;
  }

  var dbSettings = dataSource.settings || {};
  dbSettings.file = dbSettings.file || ':memory:';
  dbSettings.debug = dbSettings.debug || debug.enabled || true;

  dataSource.connector = new SQLite3(sqlite3, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    dataSource.connecting = true;
    dataSource.connector.connect(callback);
  }

};

/**
 * SQLite3 connector constructor
 *
 * @param {SQLite3} sqlite3 SQLite3 node.js binding
 * @options {Object} settings An object for the data source settings.
 * @property {String} file Path to DB or `:memory`:
 *
 * @constructor
 */
function SQLite3(sqlite3, settings) {
  this.constructor.super_.call(this, 'sqlite3', settings);
  this.name = 'sqlite3';
  // this._models = {};
  this.sqlite3 = sqlite3;
  this.settings = settings;
  if (settings.debug) {
    debug('Settings %j', settings);
    this.sqlite3.verbose();
  }
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(SQLite3, SqlConnector);

SQLite3.prototype.debug = function() {
  if (this.settings.debug) {
    debug.apply(debug, arguments);
  }
};

/**
 * Connect to SQLite3
 * @callback {Function} [callback] The callback after the connection is
 * established
 */
SQLite3.prototype.connect = function(callback) {
  var self = this;

  if (!self._defaultConnection) {
    self._getConnection(function(err, connection) {
      if (err) {
        if (callback) callback(err);
        return;
      }
      self._defaultConnection = connection;
      callback(null, self._defaultConnection);
    });
  } else {
    callback(null, self._defaultConnection);
  }
};

/**
 * Internal method to get a connection
 * @param {function} callback
 * @private
 */
SQLite3.prototype._getConnection = function(callback) {
  var self = this;

  var mode = self.settings.readonly ? self.sqlite3.OPEN_READONLY :
    self.sqlite3.OPEN_READWRITE | self.sqlite3.OPEN_CREATE;
  var db = new self.sqlite3.Database(self.settings.file, mode, function(err) {
    if (err) return callback(err);
    callback(null, db);
  });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @callback {Function} [callback] The callback after the SQL statement is
 * executed
 * @param {String|Error} err The error string or object
 * @param {Object[]} data The result from the SQL
 */
SQLite3.prototype.executeSQL = function(sql, params, options, callback) {
  var self = this;

  if (callback === undefined && typeof params === 'function') {
    callback = params;
    params = [];
    options = {};
  }

  if (callback === undefined && typeof options === 'function') {
    callback = options;
    options = {};
  }

  if (params && params.length > 0) {
    debug('SQL: %s\nParameters: %j', sql, params);
  } else {
    debug('SQL: %s', sql);
  }

  // SQLite doesn't type check so we do this in the connector instead.
  // See SQLite3.prototype.toColumnValue
  for (var p in params) {
    if (!params.hasOwnProperty(p)) continue;
    if (params[p] instanceof InvalidParam)
      params[p] = null;
  }

  var transaction = options.transaction;
  if (transaction && transaction.connection &&
    transaction.connector === this) {
    debug('Execute SQL within a transaction');
    executeWithConnection(transaction.connection);
  } else {
    self.connect(function(err, connection) {
      if (err) {
        debug('closing connection due to error: %s', err.message);
        if (connection === self._defaultConnection) {
          self._defaultConnection.close();
          self._defaultConnection = null;
        }
        return callback(err);
      }

      executeWithConnection(connection);
    });
  }


  function executeWithConnection(client) {
    var stmtType = sql.trim().toLowerCase();

    if (stmtType.indexOf('select') === 0 || stmtType.indexOf('pragma') === 0) {
      client.all(sql, params, processResult);
    } else {
      client.run(sql, params, processResult);
    }

    function processResult(err, rows) {
      if (err) {
        debug('Error running sql `%s`: %j', sql, err);
      } else {
        debug('execute result: %j %j', this, rows);
      }

      var result = {};
      if (rows) {
        result = rows;
      }

      if (this.hasOwnProperty('changes')) {
        result.count = this.changes;
      }

      if (this.hasOwnProperty('lastID')) {
        result.lastID = this.lastID;
      }

      // XXX: Workaround one of the SQL driver tests which expects /duplicate/
      // to be part of error string
      if (err && err.message.match(/UNIQUE constraint failed/i)) {
        err.message += ' (duplicate value)';
      }

      callback(err, result);
    }
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
SQLite3.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  return name.toLowerCase();
};

/*!
 * Escape the name for SQLite3 DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
SQLite3.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }

  name.replace(/["]/g, '""');
  return '"' + name + '"';
};

SQLite3.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  return value;
};

function buildLimit(filter) {
  var clause = [];
  if (isNaN(filter.limit)) {
    filter.limit = 0;
  }
  if (isNaN(filter.offset)) {
    filter.offset = 0;
  }
  if (!filter.limit && !filter.offset) {
    return '';
  }
  if (filter.limit) {
    clause.push('LIMIT ' + filter.limit);
  }
  if (filter.offset) {
    clause.push('OFFSET ' + filter.offset);
  }
  return clause.join(' ');
}

SQLite3.prototype.applyPagination = function(model, sql, filter) {
  var limitClause = buildLimit(filter);
  return limitClause ? sql.merge(limitClause) : sql;
};

/**
 * Disconnect from SQLite3
 * @param {Function} [cb] The callback function
 */
SQLite3.prototype.disconnect = function disconnect(cb) {
  if (cb) {
    process.nextTick(cb);
  }
};

SQLite3.prototype.getInsertedId = function(model, info) {
  return info.lastID;
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
SQLite3.prototype.getPlaceholderForIdentifier = function(key) {
  return '$' + key;
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
SQLite3.prototype.getPlaceholderForValue = function(key) {
  return '?' + key;
};

SQLite3.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.count;
};

/**
 * Create the table for the given model
 * @param {String} model The model name
 * @param {Object} [options] options
 * @param {Function} [cb] The callback function
 */
SQLite3.prototype.createTable = function(model, options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = null;
  }
  options = options || {};

  var tableName = this.tableEscaped(model);
  var self = this;

  var stmts = [];
  var colDefs = self.buildColumnDefinitions(model);
  if (colDefs instanceof Error) return cb(colDefs);

  stmts.push(fmt('CREATE TABLE %s (%s)', tableName, colDefs));
  stmts = stmts.concat(self._buildColumnIndexes(model));

  async.eachSeries(stmts, function(stmt, cb) {
    self.executeSQL(stmt, [], options, cb);
  }, cb);
};

SQLite3.prototype.buildColumnDefinitions = function(model) {
  var properties = this.getModelDefinition(model).properties;
  var line = [];
  for (var propertyName in properties) {
    if (!properties.hasOwnProperty(propertyName)) continue;
    var colDef = this._buildColumnDefinition(model, propertyName);
    if (colDef instanceof Error) return colDef;
    line.push(colDef);
  }
  return line.join(',');
};

SQLite3.prototype._buildColumnDefinition = function(model, propertyName) {
  var property = this.getModelDefinition(model).properties[propertyName];

  var defaultClause = this._getDefaultClause(property);
  if (defaultClause instanceof Error) return defaultClause;
  var columnName = this.column(model, propertyName);

  var line = '"' + columnName + '" ' +
    this._columnDataType(model, propertyName) +
    defaultClause +
    (this.isNullable(property) ? '' : ' NOT NULL') +
    (property.id ? ' PRIMARY KEY' : '');
  return line;
};

function _convertBoolean(value) {
  var booleanTrue = ['t', 'T', 'y', 'Y', '1', 1, true];
  var booleanFalse = ['f', 'F', 'n', 'N', '0', 0, false];

  if (booleanTrue.indexOf(value) !== -1)
    return 1;
  if (booleanFalse.indexOf(value) !== -1)
    return 0;
  return Error('SQLITE3: Invalid boolean default: ' + value);
}

SQLite3.prototype._getDefaultClause = function(property) {
  if (!property || !property.sqlite3 || !property.sqlite3.dbDefault) return '';
  var value = property.sqlite3.dbDefault;

  switch (property.type.name) {
    case 'Number':
      if (isNaN(value)) return Error('Invalid numeric default: ' + value);
      return ' DEFAULT ' + Number(value);
    case 'Boolean':
      return 'DEFAULT ' + _convertBoolean(value);
    case 'Date':
      if (value === 'now') {
        // store as ms since 1970
        return ' DEFAULT (CAST(STRFTIME(\'%s\', \'now\') AS INTEGER)*1000)';
      }
      if (!isNaN(value))
        return ' DEFAULT ' + value;
      if (typeof value === 'string') {
        if (!isNaN(value))
          return ' DEFAULT ' + Number(value);
        var parsedValue = moment(value);
        if (!parsedValue.isValid())
          return ' DEFAULT ' + (parsedValue.unix() * 1000); // store as ms
      }
      return Error('Invalid date default: ' + value);
    case 'String':
      return 'DEFAULT "' + this.escapeValue(value) + '"';
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Array':
    case 'Object':
    case 'JSON':
    default:
      return Error('Default value for ' + property.type.name +
        ' is not supported');
  }
};

SQLite3.prototype._columnDataType = function(model, propertyName) {
  var columnMetadata = this.columnMetadata(model, propertyName);
  var colType = columnMetadata && columnMetadata.dataType;
  if (colType) {
    colType = colType.toUpperCase();
  }
  var property = this.getModelDefinition(model).properties[propertyName];
  if (!property) {
    return null;
  }
  var colLength = columnMetadata && columnMetadata.dataLength ||
    property.length || property.limit;
  if (colType && colLength) {
    return colType + '(' + colLength + ')';
  }
  return this._buildColumnType(property);
};

SQLite3.prototype._buildColumnType = function(property) {
  switch (property.type.name) {
    case 'Number':
      if (property.id)
        return 'INTEGER';
      return 'REAL';
    case 'Boolean':
    case 'Date':
      return 'INTEGER';
    case 'String':
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Array':
    case 'Object':
    case 'JSON':
      return 'TEXT';
    default:
      //if (!Array.isArray(prop.type) && !prop.type.modelName) {
      //  return 'TEXT';
      //}
      return 'TEXT';
  }
};

SQLite3.prototype._buildColumnIndexes = function(model) {
  var properties = this.getModelDefinition(model).properties;
  var settings = this.getModelDefinition(model).settings;
  var indexes = [];
  var self = this;

  function escape(name) {
    return self.escapeName(name.toLowerCase());
  }

  for (var propertyName in properties) {
    if (!properties.hasOwnProperty(propertyName) || !propertyName) continue;
    var property = properties[propertyName];
    if (property.index)
      indexes.push(fmt('CREATE INDEX IF NOT EXISTS %s ON %s (%s)',
        escape(model + '_' + propertyName),
        self.tableEscaped(model), escape(propertyName)
      ));
  }
  if (settings && settings.indexes) {
    for (var index in settings.indexes) {
      if (!settings.indexes.hasOwnProperty(index) || !index) continue;

      var columns;
      if (settings.indexes[index].columns)
        columns = settings.indexes[index].columns.split(',');
      else if (settings.indexes[index].keys) {
        if (settings.indexes[index].keys instanceof Array) {
          columns = settings.indexes[index].keys;
        } else {
          columns = Object.keys(settings.indexes[index].keys);
        }
      } else {
        debug('Unable to locate keys for index in settings: %j',
          settings.indexes[index]
        );
      }

      if (!columns) continue;
      for (var i in columns) {
        if (!columns.hasOwnProperty(i)) continue;
        columns[i] = self.dbName(columns[i]).trim();
      }

      indexes.push(fmt('CREATE INDEX IF NOT EXISTS %s ON %s (%s)',
        escape(index || model + '_' + columns.join('_')),
        self.tableEscaped(model), columns.map(escape).join(',')
      ));
    }
  }
  return indexes;
};

SQLite3.prototype.toColumnValue = function(property, value) {
  if (value == null) {
    return null;
  }
  if (!property) {
    return value;
  }
  switch (property.type.name) {
    case 'Number':
      if (isNaN(value))
        return new InvalidParam('SQLITE3: Invalid number: ' + value);
      return Number(value);
    case 'Boolean':
      var b = _convertBoolean(value);
      return b;
    case 'String':
      return value;
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Object':
    case 'ModelConstructor':
      return JSON.stringify(value);
    case 'JSON':
      try {
        JSON.parse(value);
      } catch (e) {
        return new InvalidParam('SQLITE3: Invalid JSON: ' + value);
      }
      return String(value);
    case 'Date':
      if (!isNaN(value))
        return Number(value);
      if (typeof value === 'string') {
        var parsedValue = moment(value);
        if (!parsedValue.isValid())
          return parsedValue.unix() * 1000; // store as ms
      }
      return new InvalidParam('SQLITE3: Invalid JSON: ' + value);
    case 'Array':
    default:
      return value;
  }
};

SQLite3.prototype.fromColumnValue = function(property, value) {
  if (value === null || !property) {
    return value;
  }
  switch (property.type.name) {
    case 'Number':
      return (+value);
    case 'Boolean':
      return (
        value === 'Y' || value === 'y' ||
        value === 'T' || value === 't' ||
        value === '1' || value === 1
      );
    case 'String':
      return String(value);
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Array':
    case 'Object':
    case 'ModelConstructor':
      return JSON.parse(value);
    case 'JSON':
      return String(value);
    case 'Date':
      return new Date(value);
    default:
      return value;
  }
};

// Used when inserting an empty row
SQLite3.prototype.buildInsertDefaultValues = function() {
  return 'DEFAULT VALUES';
};


//require('./discovery')(SQLite3);
require('./migration')(SQLite3);
require('./transaction')(SQLite3);
