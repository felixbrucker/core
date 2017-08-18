'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var levelup = require('levelup');
var path = require('path');
var assert = require('assert');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
var fs = require('fs');
var path = require('path');

/**
 * Implements an LevelDB/file store storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Path to store the level db
 * @constructor
 * @license AGPL-3.0
 */
function EmbeddedStorageAdapter2(storageDirPath) {
  if (!(this instanceof EmbeddedStorageAdapter2)) {
    return new EmbeddedStorageAdapter2(storageDirPath);
  }

  this._validatePath(storageDirPath);

  this._path = storageDirPath;
  this._dataPath = path.join(storageDirPath, 'data');
  this._db = levelup(path.join(this._path, 'contracts.db'), {
    maxOpenFiles: EmbeddedStorageAdapter2.MAX_OPEN_FILES
  });
  fs.mkdirSync(this._dataPath);
  this._isOpen = true;
}

EmbeddedStorageAdapter2.SIZE_START_KEY = '0';
EmbeddedStorageAdapter2.SIZE_END_KEY = 'z';
EmbeddedStorageAdapter2.MAX_OPEN_FILES = 1000;

inherits(EmbeddedStorageAdapter2, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
EmbeddedStorageAdapter2.prototype._validatePath = function(storageDirPath) {
  if (!utils.existsSync(storageDirPath)) {
    mkdirp.sync(storageDirPath);
  }

  assert(utils.isDirectory(storageDirPath), 'Invalid directory path supplied');
};

/**
 * Implements the abstract {@link StorageAdapter#_get}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._get = function(key, callback) {

  this._db.get(key, { fillCache: false }, function(err, value) {
    if (err) {
      return callback(err);
    }

    var result = JSON.parse(value);
    var fskey = result.fskey || key;

    fs.createReadStream(path.join(this._dataPath, fskey), function(err, rstream) {
      function _getShardStreamPointer(stream, callback) {
        result.shard = stream;

        callback(null, result);
      }

      if (err) { // doesnt exist
        fskey = utils.rmd160(key, 'hex');
        result.fskey = fskey;
        fs.createWriteStream(path.join(this._dataPath, fskey), (err, wstream) => {
          if (err) {
            return callback(err);
          }
          return _getShardStreamPointer(wstream, callback);
        });
      } else {
        return _getShardStreamPointer(rstream, callback);
      }
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._peek = function(key, callback) {
  this._db.get(key, { fillCache: false }, function(err, value) {
    if (err) {
      return callback(err);
    }

    callback(null, JSON.parse(value));
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._put = function(key, item, callback) {
  var self = this;

  item.shard = null; // NB: Don't store any shard data here

  item.fskey = utils.rmd160(key, 'hex');

  self._db.put(key, JSON.stringify(item), {
    sync: true
  }, function(err) {
    if (err) {
      return callback(err);
    }

    callback(null);
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._del = function(key, callback) {
  var self = this;
  var fskey = key;

  self._peek(key, function(err, item) {
    if (!err && item.fskey) {
      fskey = item.fskey;
    }

    self._db.del(key, function(err) {
      if (err) {
        return callback(err);
      }

      fs.unlink(path.join(this._dataPath, fskey), function(err) {
        if (err) {
          return callback(err);
        }

        callback(null);
      });
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._flush = function(callback) {
  // this._fs.flush(callback);
  // not needed?
  callback();
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._size = function(key, callback) {

  if (typeof key === 'function') {
    callback = key;
    key = null;
  }

  this._db.db.approximateSize(
    EmbeddedStorageAdapter2.SIZE_START_KEY,
    EmbeddedStorageAdapter2.SIZE_END_KEY,
    function(err, contractDbSize) {
      if (err) {
        return callback(err);
      }

      function handleStatResults(err, stats) {
        if (err) {
          return callback(err);
        }

        var size = stats.size;

        callback(null, size, contractDbSize);
      }

      /* istanbul ignore if */
      if (key) {
        fs.stat(path.join(this._dataPath, utils.rmd160(key, 'hex')), handleStatResults);
      } else {
        fs.stat(this._dataPath, handleStatResults);
      }
    }
  );
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
EmbeddedStorageAdapter2.prototype._keys = function() {
  return this._db.createKeyStream();
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._open = function(callback) {
  var self = this;

  if (!this._isOpen) {
    return this._db.open(function(err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = true;
      callback(null);
    });
  }

  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
EmbeddedStorageAdapter2.prototype._close = function(callback) {
  var self = this;

  if (this._isOpen) {
    return this._db.close(function(err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = false;
      callback(null);
    });
  }

  callback(null);
};

module.exports = EmbeddedStorageAdapter2;
