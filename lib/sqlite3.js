/*!
 * SQLite3 connector for LoopBack
 */
var sqlite3 = require('sqlite3');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var util = require('util');
var debug = require('debug')('loopback:connector:sqlite3');

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
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;
  dbSettings.debug = dbSettings.debug || debug.enabled;

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
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the SQLite3 DB server
 * @property {Number} port The port number of the SQLite3 DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function SQLite3(sqlite3, settings) {
  // this.name = 'sqlite3';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'sqlite3', settings);
  this.clientConfig = settings.url || settings;
  this.pg = sqlite3;
  this.settings = settings;
  if (settings.debug) {
    debug('Settings %j', settings);
  }
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(SQLite3, SqlConnector);

SQLite3.prototype.debug = function() {
  if (this.settings.debug) {
    debug.apply(debug, arguments);
  }
};

SQLite3.prototype.getDefaultSchemaName = function() {
  return 'public';
};

/**
 * Connect to SQLite3
 * @callback {Function} [callback] The callback after the connection is established
 */
SQLite3.prototype.connect = function(callback) {
  var self = this;
  self.pg.connect(self.clientConfig, function(err, client, done) {
    self.client = client;
    process.nextTick(done);
    callback && callback(err, client);
  });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @callback {Function} [callback] The callback after the SQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[]) data The result from the SQL
 */
SQLite3.prototype.executeSQL = function(sql, params, options, callback) {
  var self = this;

  if (callback === undefined && typeof params === 'function') {
    callback = params;
    params = [];
  }

  if (self.settings.debug) {
    if (params && params.length > 0) {
      self.debug('SQL: %s\nParameters: %j', sql, params);
    } else {
      self.debug('SQL: %s', sql);
    }
  }

  self.pg.connect(self.clientConfig, function(err, client, done) {
    client.query(sql, params, function(err, data) {
      // if(err) console.error(err);
      if (err && self.settings.debug) {
        self.debug(err);
      }
      if (self.settings.debug && data) self.debug("%j", data);
      process.nextTick(done); // Release the pooled client
      var result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = {count: data.rowCount};
            break;
          default:
            result = data.rows;
        }
      }
      callback(err ? err : null, result);
    });
  });
};

/*
 * Check if the connection is in progress
 * @private
 */
function stillConnecting(dataSource, obj, args) {
  if (dataSource.connected) return false; // Connected

  var method = args.callee;
  // Set up a callback after the connection is established to continue the method call
  dataSource.once('connected', function() {
    method.apply(obj, [].slice.call(args));
  });
  if (!dataSource.connecting) {
    dataSource.connect();
  }
  return true;
}

/**
 * Execute a sql statement with the given parameters
 *
 * @param {String} sql The SQL statement
 * @param {[]} params An array of parameter values
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object[]} data The result from the SQL
 */
SQLite3.prototype.query = function(sql, params, callback) {
  if (stillConnecting(this.dataSource, this, arguments)) return;

  if (!callback && typeof params === 'function') {
    callback = params;
    params = [];
  }

  params = params || [];

  var cb = callback || function(err, result) {
    };
  this.execute(sql, params, cb);
};

/*!
 * Categorize the properties for the given model and data
 * @param {String} model The model name
 * @param {Object} data The data object
 * @returns {{ids: String[], idsInData: String[], nonIdsInData: String[]}}
 * @private
 */
SQLite3.prototype._categorizeProperties = function(model, data) {
  var ids = this.idNames(model);
  var idsInData = ids.filter(function(key) {
    return data[key] !== null && data[key] !== undefined;
  });
  var props = Object.keys(this.getModelDefinition(model).properties);
  var nonIdsInData = Object.keys(data).filter(function(key) {
    return props.indexOf(key) !== -1 && ids.indexOf(key) === -1 && data[key] !== undefined;
  });
  return {
    ids: ids,
    idsInData: idsInData,
    nonIdsInData: nonIdsInData
  };
};

SQLite3.prototype.mapToDB = function(model, data) {
  var dbData = {};
  if (!data) {
    return dbData;
  }
  var props = this.getModelDefinition(model).properties;
  for (var p in data) {
    if (props[p]) {
      var pType = props[p].type && props[p].type.name;
      if (pType === 'GeoPoint' && data[p]) {
        dbData[p] = '(' + data[p].lat + ',' + data[p].lng + ')';
      } else {
        dbData[p] = data[p];
      }
    }
  }
  return dbData;
}

SQLite3.prototype.buildInsertReturning = function(model, data, options) {
  var idColumnNames = [];
  var idNames = this.idNames(model);
  for (var i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

SQLite3.prototype.buildInsertDefaultValues = function(model, data, options) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// sqlite3 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-sqlite3/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 SQLite3.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

SQLite3.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  var type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
      val === 't' || val === '1');
    }
  } else if (prop && type === 'GeoPoint' || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      var point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1]
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now pg driver converts point to {x: lat, y: lng}
      return {
        lat: val.x,
        lng: val.y
      };
    } else {
      return val;
    }
  } else {
    return val;
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
  // SQLite3 default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  var escaped = '"';
  for(var i = 0; i < str.length; i++) {
    var c = str[i];
    if(c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  var hasBackslash = false;
  var escaped = '\'';
  for(var i = 0; i < str.length; i++) {
    var c = str[i];
    if(c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if(hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*!
 * Escape the name for SQLite3 DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
SQLite3.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

SQLite3.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  return value;
};

SQLite3.prototype.tableEscaped = function(model) {
  var schema = this.schema(model) || 'public';
  return this.escapeName(schema) + '.' +
    this.escapeName(this.table(model));
};

/*!
 * Get a list of columns based on the fields pattern
 *
 * @param {String} model The model name
 * @param {Object|String[]} props Fields pattern
 * @returns {String}
 */
SQLite3.prototype.getColumns = function(model, props) {
  var cols = this.getModelDefinition(model).properties;
  var self = this;
  var keys = Object.keys(cols);
  if (Array.isArray(props) && props.length > 0) {
    // No empty array, including all the fields
    keys = props;
  } else if ('object' === typeof props && Object.keys(props).length > 0) {
    // { field1: boolean, field2: boolean ... }
    var included = [];
    var excluded = [];
    keys.forEach(function(k) {
      if (props[k]) {
        included.push(k);
      } else if ((k in props) && !props[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        var index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  var names = keys.map(function(c) {
    return self.columnEscaped(model, c);
  });
  return names.join(', ');
};

function buildLimit(limit, offset) {
  var clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

SQLite3.prototype.applyPagination =
  function(model, sql, limit, offset, order) {
    var limitClause = buildLimit(limit, offset);
    return limitClause ? sql + ' ' + limitClause : sql;
  };

SQLite3.prototype.buildExpression =
  function(columnName, operator, columnValue, propertyValue) {
    if (propertyValue instanceof RegExp) {
      columnValue = "'" + propertyValue.source + "'";
      if (propertyValue.ignoreCase) {
        return new ParameterizedSQL(columnName + ' ~* ?', [columnValue]);
      } else {
        return new ParameterizedSQL(columnName + ' ~ ?', [columnValue]);
      }
    }
    switch(operator) {
      case 'like':
        return new ParameterizedSQL({
          sql: columnName + " LIKE ? ESCAPE '\\'",
          params: [columnValue]
        });
      case 'nlike':
        return new ParameterizedSQL({
          sql: columnName + " NOT LIKE ? ESCAPE '\\'",
          params: [columnValue]
        });
      default:
        // Invoke the base implementation of `buildExpression`
        var exp = this.invokeSuper('buildExpression',
          columnName, operator, columnValue, propertyValue);
        return exp;
    }
  };

/**
 * Disconnect from SQLite3
 * @param {Function} [cb] The callback function
 */
SQLite3.prototype.disconnect = function disconnect(cb) {
  if (this.pg) {
    if (this.settings.debug) {
      this.debug('Disconnecting from ' + this.settings.hostname);
    }
    var pg = this.pg;
    this.pg = null;
    pg.end();  // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

SQLite3.prototype.ping = function(cb) {
  this.query('SELECT 1 AS result', [], cb);
}

function escape(val) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean':
      return (val) ? "true" : "false";
    case 'number':
      return val + '';
  }

  if (typeof val === 'object') {
    val = (typeof val.toISOString === 'function')
      ? val.toISOString()
      : val.toString();
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
    switch (s) {
      case "\0":
        return "\\0";
      case "\n":
        return "\\n";
      case "\r":
        return "\\r";
      case "\b":
        return "\\b";
      case "\t":
        return "\\t";
      case "\x1a":
        return "\\Z";
      case "\'":
        return "''"; // For sqlite3
      case "\"":
        return s; // For sqlite3
      default:
        return "\\" + s;
    }
  });
  // return "q'#"+val+"#'";
  return "'" + val + "'";
}

function generateQueryParams(data, props) {
  var queryParams = [];

  function pushToQueryParams(key) {
    queryParams.push(data[key] !== undefined ? data[key] : null);
  }

  props.nonIdsInData.forEach(pushToQueryParams);
  props.idsInData.forEach(pushToQueryParams);

  return queryParams;
}

SQLite3.prototype.getInsertedId = function(model, info) {
  var idColName = this.idColumn(model);
  var idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
SQLite3.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
    // SQLite3 complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    }
    else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    var iso = val.toISOString();

    return new ParameterizedSQL({
      // 'to_date(?,\'yyyy-mm-dd hh24:mi:ss\')',
      sql: 'to_timestamp(?,\'yyyy-mm-dd hh24:mi:ss.ms\')',
      params: [iso]
    });
  }

  // SQLite3 support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      params: [val.lat, val.lng]
    });
  }

  return val;
}

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
SQLite3.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error('Placeholder for identifiers is not supported');
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
SQLite3.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

SQLite3.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.count;
};

SQLite3.prototype.buildColumnDefinition = function(model, prop) {
  var p = this.getModelDefinition(model).properties[prop];
  var line = this.columnDataType(model, prop) + ' ' +
    (this.isNullable(p) ? 'NULL' : 'NOT NULL');
  return line;
};

SQLite3.prototype.columnDataType = function(model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colType = columnMetadata && columnMetadata.dataType;
  if (colType) {
    colType = colType.toUpperCase();
  }
  var prop = this.getModelDefinition(model).properties[property];
  if (!prop) {
    return null;
  }
  var colLength = columnMetadata && columnMetadata.dataLength ||
    prop.length || prop.limit;
  if (colType && colLength) {
    return colType + '(' + colLength + ')';
  }
  return this.buildColumnType(prop);
};

require('./discovery')(SQLite3);
require('./migration')(SQLite3);
