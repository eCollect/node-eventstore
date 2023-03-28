var util = require('util'),
  fs = require('fs'),
  Store = require('../base'),
  _ = require('lodash'),
  async = require('async'),
  redis = Store.use('redis'),
  jsondate = require('jsondate'),
  debug = require('debug')('eventstore:store:redis');

const { resolvify, rejectify } = require('../helpers').async(debug);

function Redis(options) {
  options = options || {};

  Store.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 6379,
    prefix: 'eventstore',
    eventsCollectionName: 'events',
    snapshotsCollectionName: 'snapshots',
    retry_strategy: function (/* retries, cause */) {
      return false;
    },
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  if (options.url) {
    var url = require('url').parse(options.url);
    if (url.protocol === 'redis:') {
      if (url.auth) {
        var userparts = url.auth.split(':');
        options.user = userparts[0];
        if (userparts.length === 2) {
          options.password = userparts[1];
        }
      }
      options.host = url.hostname;
      options.port = url.port;
      if (url.pathname) {
        options.db = url.pathname.replace('/', '', 1);
      }
    }
  }

  this.options = options;
}

util.inherits(Redis, Store);

// helpers
function handleResultSet(err, res, callback) {
  if (err) {
    debug(err);
    return callback(err);
  }

  if (!res || res.length === 0) {
    return callback(null, []);
  }
  var arr = [];

  res.forEach(function (item) {
      arr.push(jsondate.parse(item));
  });

  callback(null, arr);
}

_.extend(Redis.prototype, {

  connect: function (callback) {
    var self = this;

    var options = this.options;

    this.client = redis.createClient({
			socket: {
				port: options.port || options.socket,
				host: options.host,
        reconnectStrategy: options.retry_strategy,
			},
			database: options.db,
      username: options.username,
      password: options.password,
			// legacyMode: true,
		});

    var calledBack = false;

    this.client.on('end', function () {
      self.disconnect();
      self.stopHeartbeat();
    });

    this.client.on('error', function (err) {
      debug(err);

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });

    this._connect().then(() => {
      self.emit('connect');

      if (self.options.heartbeat) {
        self.startHeartbeat();
      }

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });
  },

  _connect: async function() {
		if (!this.client.isOpen)
			await this.client.connect();
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    if (this.heartbeatInterval)
        return;

    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error('Heartbeat timeouted after ' + gracePeriod + 'ms (redis)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.client.ping().then(() => {
          if (graceTimer) clearTimeout(graceTimer);
      }).catch((err) => {
          if (graceTimer) clearTimeout(graceTimer);
          console.error(err.stack || err);
          self.disconnect();
      });
    }, this.options.heartbeat);
  },

  disconnect: function (callback) {
    this.stopHeartbeat();

    if (this.client && this.client.isOpen)
        this.client.quit();

    this.emit('disconnect');
    if (callback) callback(null, this);
  },

  clear: function (callback) {
    const deletePromises = [
        this.client.del('nextItemId:' + this.options.prefix),
    ];

    this.client.keys(this.options.prefix + ':*').then((keys) => {
        for (const key of keys) {
          deletePromises.push(this.client.del(key));
        }
    }),

    this._connect()
      .then(() => {
          Promise.all(deletePromises).then(resolvify(callback))
      })
      .catch(rejectify(callback));
  },

  _getNewIdAsync: async function() {
    await this._connect();
    const id = await this.client.incr('nextItemId:' + this.options.prefix);
    return id.toString();
  },

  getNewId: function (callback) {
    this._getNewIdAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _addEventsAsync: async function(events) {
    var aggregateId = events[0].aggregateId;
    var aggregate = events[0].aggregate || '_general';
    var context = events[0].context || '_general';

    function eventKey(event) {
      return event.commitStamp.getTime() + ':' + event.commitSequence.toString() + ':' + context + ':' + aggregate + ':' + aggregateId + ':' + event.id;
    }

    var prefix = this.options.prefix + ':' + this.options.eventsCollectionName;
    var revisionKey = prefix + ':' + context + ':' + aggregate + ':' + aggregateId + ':revision';

    var multi = events.reduce((multi) => {
      return multi.incr(revisionKey);
    }, this.client.multi());

    await this._connect();
    const revisions = await multi.exec();
    var errors = revisions.filter((reply) => {
      return reply instanceof Error;
    });

    if (errors.length) {
      var message = 'error while adding events for aggregate ' + aggregate + ' ' + aggregateId;
      throw new Error(message + '\n' + errors.join('\n'));
    }

    var savedKeysAndEvents = events.map((event, index) => {
      var key = prefix + ':' + eventKey(event);
      event.streamRevision = parseInt(revisions[index], 10) - 1;
      event.applyMappings();
      return [key, JSON.stringify(event)];
    });

    var undispatchedKeysAndEvents = events.map((event) => {
      var key = this.options.prefix + ':undispatched_' + this.options.eventsCollectionName + ':' + eventKey(event);
      return [key, JSON.stringify(event)];
    });

    return this.client.mSet({
      ..._.fromPairs(savedKeysAndEvents),
      ..._.fromPairs(undispatchedKeysAndEvents),
    });
  },

  addEvents: function (events, callback) {
    var noAggId = events.filter(function (event) {
      return !event.aggregateId
    }).length > 0;

    if (noAggId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    if (!events || events.length === 0) {
      return callback(null);
    }

    this._addEventsAsync(events)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _scan: async function (pattern, handleKeys) {
      await this._connect();

      for await (const key of this.client.scanIterator({
          MATCH: pattern,
      })) {
          await handleKeys(key);
      }
  },

  _getEventsAsync: async function (query, skip, limit) {
    var aggregateId = query.aggregateId || '*';
    var aggregate = query.aggregate || '*';
    var context = query.context || '*';

    var allKeys = [];

    const key = this.options.prefix + ':' + this.options.eventsCollectionName + ':*:*:' + context + ':' + aggregate + ':' + aggregateId + ':*';
    await this._scan(key, async (key) => {
        allKeys.push(key);
      }
    );

    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    if (limit === -1) {
      allKeys = allKeys.slice(skip);
    } else {
      allKeys = allKeys.slice(skip, skip + limit);
    }

    if (allKeys.length === 0)
      return [];

    return this.client.mGet(allKeys);
  },

  getEvents: function (query, skip, limit, callback) {
    this._getEventsAsync(query, skip, limit)
      .then((res) => {
        handleResultSet(null, res, callback);
      }).catch((err) => {
        handleResultSet(err, null, callback);
      });
  },

  _getEventsSinceAsync: async function (date, skip, limit) {
    var allKeys = [];

    const key = this.options.prefix + ':' + this.options.eventsCollectionName + ':*:*:*:*:*:*';
    await this._scan(key, async (key) => {
        var parts = key.split(':');
        var timePart = parseInt(parts[2], 10);
        if (timePart >= date.getTime())
            allKeys.push(key);
      });

    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    if (limit === -1) {
      allKeys = allKeys.slice(skip);
    }
    else {
      allKeys = allKeys.slice(skip, skip + limit);
    }

    if (allKeys.length === 0)
      return [];

    return this.client.mGet(allKeys);
  },

  getEventsSince: function (date, skip, limit, callback) {
    this._getEventsSinceAsync(date, skip, limit)
      .then((res) => {
        handleResultSet(null, res, callback);
      }).catch((err) => {
        handleResultSet(err, null, callback);
      });
  },

  _getEventsByRevisionAsync: async function (query, revMin, revMax) {
    var aggregateId = query.aggregateId;
    var aggregate = query.aggregate || '*';
    var context = query.context || '*';

    var allKeys = [];

    const key = this.options.prefix + ':' + this.options.eventsCollectionName + ':*:*:' + context + ':' + aggregate + ':' + aggregateId + ':*';
    await this._scan(key, async (key) => {
        allKeys.push(key);
    });

    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    if (revMax === -1) {
      allKeys = allKeys.slice(revMin);
    } else {
      allKeys = allKeys.slice(revMin, revMax);
    }

    if (allKeys.length === 0)
      return [];

    return this.client.mGet(allKeys);
  },

  getEventsByRevision: function (query, revMin, revMax, callback) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    this._getEventsByRevisionAsync(query, revMin, revMax)
      .then((res) => {
        handleResultSet(null, res, callback);
      }).catch((err) => {
        handleResultSet(err, null, callback);
      });
  },

  _getLastEventAsync: async function(query) {
      var aggregateId = query.aggregateId || '*';
      var aggregate = query.aggregate || '*';
      var context = query.context || '*';

      var allKeys = [];

      const key = this.options.prefix + ':' + this.options.eventsCollectionName + ':*:*:' + context + ':' + aggregate + ':' + aggregateId + ':*';
      await this._scan(key, async (key) => {
          allKeys.push(key);
      });
      
      allKeys = _.sortBy(allKeys, function (s) {
        return s;
      });

      if (allKeys.length === 0)
        return null;

      return this.client.mGet(allKeys);
  },

  getLastEvent: function (query, callback) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    this._getLastEventAsync(query).then((res) => {
        handleResultSet(null, res, function (err, evts) {
          if (evts.length === 0) return callback(null, null);
          callback(null, evts[evts.length - 1])
        });
    }).catch((err) => {
        handleResultSet(err, res, callback);
    })
  },

  _getUndispatchedEventsAsync: async function (query) {
      var aggregateId = '*';
      var aggregate = '*';
      var context = '*';

      if (query) {
        aggregateId = query.aggregateId || '*';
        aggregate = query.aggregate || '*';
        context = query.context || '*';
      }

      var evts = [];

      const key = this.options.prefix + ':undispatched_' + this.options.eventsCollectionName + ':*:*:' + context + ':' + aggregate + ':' + aggregateId + ':*';
      await this._scan(key, async (key) => {
          const res = await this.client.get(key);
          handleResultSet(null, [res], function (err, events) {
            evts = evts.concat(events);
          });
        }
      );

      evts = _.sortBy(evts, function (s) {
        return s.commitStamp.getTime() + ':' + s.commitSequence.toString();
      });

      return evts;
  },

  getUndispatchedEvents: function (query, callback) {
      this._getUndispatchedEventsAsync(query)
        .then(resolvify(callback))
        .catch(rejectify(callback));
  },

  _setEventToDispatched: async function (id) {
      const key = this.options.prefix + ':undispatched_' + this.options.eventsCollectionName + ':*:*:*:*:*:' + id;
      await this._scan(key, async (key) => {
          await this.client.del(key);
      });
  },

  setEventToDispatched: function (id, callback) {
    this._setEventToDispatched(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  addSnapshot: function (snap, callback) {
    if (!snap.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var aggregateId = snap.aggregateId;
    var aggregate = snap.aggregate || '_general';
    var context = snap.context || '_general';

    const key = this.options.prefix + ':' + this.options.snapshotsCollectionName + ':' 
      + snap.commitStamp.getTime() + ':' + context + ':' + aggregate + ':' + aggregateId + ':' + snap.id;

    this.client.set(key, JSON.stringify(snap))
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _cleanSnapshotsAsync: async function (query) {
      const keys = await this._scanSnapshotsAsync(query);
      
      var keysToDelete = keys
        .sort()
        .slice(0, -1 * this.options.maxSnapshotsCount)

      if (keysToDelete.length === 0)
        return 0;

      return this.client.del(keysToDelete);
  },

  cleanSnapshots: function (query, callback) {
    this._cleanSnapshotsAsync(query)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _scanSnapshotsAsync: async function (query) {
    if (!query.aggregateId)
      throw new Error('aggregateId not defined!');

    var aggregateId = query.aggregateId;
    var aggregate = query.aggregate || '*';
    var context = query.context || '*';

    var allKeys = [];

    const key = this.options.prefix + ':' + this.options.snapshotsCollectionName + ':*:' + context + ':' + aggregate + ':' + aggregateId + ':*';
    await this._scan(key, async (key) => {
        allKeys.push(key);
      }
    );

    return allKeys;
  },

  scanSnapshots: function (query, callback) {
    this._scanSnapshotsAsync(query)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getSnapshotAsync: async function (query, revMax) {
      let allKeys = await this._scanSnapshotsAsync(query);

      allKeys = _.sortBy(allKeys, function (s) {
          return s;
      }).reverse();

      if (revMax === -1) { // by default the last snapshot is kept
          allKeys = allKeys.slice(0, 1);
      }

      if (allKeys.length === 0)
          return null;
  
      // iterating recursively over snapshots, from latest to oldest
      const nextSnapshotAsync = async (key) => {
          const res = await this.client.get(key);
          var snapshot = jsondate.parse(res);
          if (revMax > -1 && snapshot.revision > revMax) {
              if (allKeys.length > 0) {
                  return nextSnapshotAsync(allKeys.shift());
              } else {
                  return null;
              }
          }
          return snapshot;
      }

      return nextSnapshotAsync(allKeys.shift());
  },

  getSnapshot: function (query, revMax, callback) {
    this._getSnapshotAsync(query, revMax)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }

});

module.exports = Redis;
