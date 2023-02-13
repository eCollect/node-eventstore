var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  stream = require('stream'),
  mongo = Store.use('mongodb'),
  mongoVersion = Store.use('mongodb/package.json').version,
  ObjectId = mongo.ObjectId,
  debug = require('debug')('eventstore:store:mongodb');

const { resolvify, rejectify } = require('../helpers').async(debug);

async function streamEventsByRevision(self, findStatement, revMin, revMax, resultStream, lastEvent) {
  findStatement.streamRevision = (revMax === -1) ? { '$gte': revMin } : { '$gte': revMin, '$lt': revMax };

  var mongoStream = self.events.find(findStatement).sort({
      commitStamp: 1,
      streamRevision: 1,
      commitSequence: 1,
  }).stream();

	for await (const e of mongoStream) {
		try {
				if (!lastEvent) {
						lastEvent = e;
						resultStream.write(lastEvent); // Should write the event to resultStream as if there's no lastEvent when there's an event in stream, the event must be first entry of this query.
						continue;
				} 
				
				// if for some reason we have written this event already
				const cond1 = (e.streamRevision === lastEvent.streamRevision && e.restInCommitStream <= lastEvent.restInCommitStream);
				const cond2 = (e.streamRevision <= lastEvent.streamRevision);
				if (!cond1 && !cond2) {
						lastEvent = e;
						resultStream.write(lastEvent);
				}
		} catch (error) {
				return resultStream.destroy(error);
		}
	};

  if (!lastEvent)
		return resultStream.end();

	var txOk = (revMax === -1 && !lastEvent.restInCommitStream) ||
							(revMax !== -1 && (lastEvent.streamRevision === revMax - 1 || !lastEvent.restInCommitStream));

	if (txOk) {
		// the following is usually unnecessary
		self.removeTransactions(lastEvent);
		resultStream.end(); // lastEvent was keep duplicated from this line. We should not re-write last event into the stream when ending it. thus end() rather then end(lastEvent).
	}

	try {
			await self.repairFailedTransaction(lastEvent);
			return streamEventsByRevision(self, findStatement, lastEvent.revMin, revMax, resultStream, lastEvent);
	} catch (err) {
		if (err.message.indexOf('missing tx entry') >= 0)
				return resultStream.end(lastEvent); // Maybe we should check on this line too?

		debug(err);
		return resultStream.destroy(error);
	}
};
  
function Mongo(options) {
  options = options || {};

  Store.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 27017,
    dbName: 'eventstore',
    eventsCollectionName: 'events',
    snapshotsCollectionName: 'snapshots',
    transactionsCollectionName: 'transactions',
    //	heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  var defaultOpt = {
    ssl: false
  };

  options.options = options.options || {};

  defaultOpt.useNewUrlParser = true;
  defaultOpt.useUnifiedTopology = true;
  _.defaults(options.options, defaultOpt);

  this.options = options;
}

util.inherits(Mongo, Store);

_.extend(Mongo.prototype, {

  _connectAsync: async function() {
    var options = this.options;

    var connectionUrl;

		if (options.url) {
			connectionUrl = options.url;
		} else {
			var members = options.servers
				? options.servers
				: [{ host: options.host, port: options.port }];

			var memberString = _(members).map(function (m) { return m.host + ':' + m.port; });
			var authString = options.username && options.password
				? options.username + ':' + options.password + '@'
				: '';
			var optionsString = options.authSource
				? '?authSource=' + options.authSource
				: '';

			connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
		}

    var client;

		client = new mongo.MongoClient(connectionUrl, options.options);	
		const cl = await client.connect();

		this.db = cl.db(cl.s.options.dbName);
		this.db.close = cl.close.bind(cl);

		cl.on('serverClosed', () => {
			this.emit('disconnect');
			this.stopHeartbeat();
		});

		this.events = this.db.collection(options.eventsCollectionName);
		this.snapshots = this.db.collection(options.snapshotsCollectionName);
		this.transactions = this.db.collection(options.transactionsCollectionName);

		this.events.createIndex({ aggregateId: 1, streamRevision: 1 }).catch(debug);
		this.events.createIndex({ commitStamp: 1 }).catch(debug);
		this.events.createIndex({ dispatched: 1 }, { sparse: true }).catch(debug);
		this.events.createIndex({ commitStamp: 1, streamRevision: 1, commitSequence: 1 }).catch(debug);

		this.snapshots.createIndex({ aggregateId: 1, revision: -1 }).catch(debug);

		this.transactions.createIndex({ aggregateId: 1, 'events.streamRevision': 1 }).catch(debug);

		this.events.createIndex({ aggregate: 1, aggregateId: 1, commitStamp: -1, streamRevision: -1, commitSequence: -1 }).catch(debug);
		
		if (options.positionsCollectionName) {
			this.positions = this.db.collection(options.positionsCollectionName);
			this.positionsCounterId = options.eventsCollectionName;
		}

		this.emit('connect');
		if (this.options.heartbeat) {
			this.startHeartbeat();
		}
  },

  connect: function (callback) {
    this._connectAsync().then(() => {
			if (callback) callback(null, this);
		}).catch(rejectify(callback))
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
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.db.admin().ping()
				.then(() => {
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

    if (!this.db) {
      if (callback) callback(null);
      return;
    }

		this.db.close().then(resolvify(callback)).catch(rejectify(callback));
  },

  clear: function (callback) {
    var self = this;

		const deletePromises = [
			self.events.deleteMany({}),
			self.snapshots.deleteMany({}),
			self.transactions.deleteMany({}),
		];

		if (self.positions)
			deletePromises.push(self.positions.deleteMany({}));

    Promise.all(deletePromises).then(resolvify(callback)).catch(rejectify(callback));
  },

  getNewId: function(callback) {
    callback(null, new ObjectId().toHexString());
  },

  getNextPositions: function(positions, callback) {
		if (!this.positions)
      return callback(null);

		this.positions.findOneAndUpdate({ 
				_id: this.positionsCounterId 
		}, { 
				$inc: { position: positions } 
		}, { 
				returnDocument: 'after',
				upsert: true,
		}).then((pos) => {
				pos.value.position += 1;
				callback(null, _.range(pos.value.position - positions, pos.value.position));
		}).catch((err) => {
				callback(err);
		});
  },

	_addEventsAsync: async function (events, commitId) {
		if (events.length === 1)
			return this.events.insertOne(events[0]);

		var tx = {
			_id: commitId,
			events: events,
			aggregateId: events[0].aggregateId,
			aggregate: events[0].aggregate,
			context: events[0].context
		};

		await this.transactions.insertOne(tx);
		await this.events.insertMany(events);
		await this._removeTransactionsAsync(events[events.length - 1]);
  },

  addEvents: function (events, callback) {
    if (events.length === 0) {
      if (callback) { callback(null); }
      return;
    }

    var commitId = events[0].commitId;

    var noAggregateId = false,
      invalidCommitId = false;

    var self = this;

		_.forEach(events, function (evt, index) {
			if (!evt.aggregateId) {
				noAggregateId = true;
			}

			if (!evt.commitId || evt.commitId !== commitId) {
				invalidCommitId = true;
			}

			evt._id = evt.id;
			evt.dispatched = false;
		});

		if (noAggregateId) {
			var errMsg = 'aggregateId not defined!';
			debug(errMsg);
			if (callback) callback(new Error(errMsg));
			return;
		}

		if (invalidCommitId) {
			var errMsg = 'commitId not defined or different!';
			debug(errMsg);
			if (callback) callback(new Error(errMsg));
			return;
		}

		this._addEventsAsync(events, commitId).then(resolvify(callback)).catch(rejectify(callback));
  },

	_makeEventsQuery: function (query, skip, limit) {
    var findStatement = {};

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    if (query.aggregateId) {
      findStatement.aggregateId = query.aggregateId;
    }

    var query = this.events.find(findStatement).sort({
        commitStamp: 1, 
        streamRevision: 1, 
        commitSequence: 1,
    });

    if (skip) {
      query.skip(skip);
    }
    
    if (limit && limit > 0) {
      query.limit(limit);
    }

    return query;
  },

  // streaming API
  streamEvents: function (query, skip, limit) {
    return this._makeEventsQuery(query, skip, limit).stream();
  },

	_makeEventsSinceQuery: function (date, skip, limit) {
		var findStatement = { commitStamp: { '$gte': date } };

    var query = this.events.find(findStatement).sort({
        commitStamp: 1, 
        streamRevision: 1,
        commitSequence: 1,
    });

    if (skip)
      query.skip(skip);
    
    if (limit && limit > 0)
      query.limit(limit);
    
    return query;
	},

  streamEventsSince: function (date, skip, limit) {
    return this._makeEventsSinceQuery(date, skip, limit).stream();
  },

  streamEventsByRevision: function (query, revMin, revMax) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement = {
      aggregateId: query.aggregateId,
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    var resultStream = new stream.PassThrough({ objectMode: true, highWaterMark: 1 });
    streamEventsByRevision(this, findStatement, revMin, revMax, resultStream);
    return resultStream;
  },

  getEvents: function (query, skip, limit, callback) {
    this._makeEventsQuery(query, skip, limit).toArray()
			.then(resolvify(callback))
			.catch(callback);
  },

  getEventsSince: function (date, skip, limit, callback) {
    this._makeEventsSinceQuery(date, skip, limit).toArray()
			.then(resolvify(callback))
			.catch(callback);
  },

	_getEventsByRevisionAsync: async function (query, revMin, revMax) {
    if (!query.aggregateId)
      throw new Error('aggregateId not defined!');

    var streamRevOptions = { '$gte': revMin, '$lt': revMax };
    if (revMax === -1) {
      streamRevOptions = { '$gte': revMin };
    }

    var findStatement = {
      aggregateId: query.aggregateId,
      streamRevision: streamRevOptions
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    const res = await this.events.find(findStatement).sort({
        commitStamp: 1, 
        streamRevision: 1, 
        commitSequence: 1, 
    }).toArray();

		if (!res || res.length === 0)
			return [];

		var lastEvt = res[res.length - 1];

		var txOk = (revMax === -1 && !lastEvt.restInCommitStream) ||
								(revMax !== -1 && (lastEvt.streamRevision === revMax - 1 || !lastEvt.restInCommitStream));

		if (txOk) {
			// the following is usually unnecessary
			this.removeTransactions(lastEvt);
			return res;
		}

		try {
			await this._repairFailedTransactionAsync(lastEvt);
			return this._getEventsByRevisionAsync(query, revMin, revMax);
		} catch (err) {
			if (err.message.indexOf('missing tx entry') >= 0)
					return res;
			throw err;
		}
  },

  getEventsByRevision: function (query, revMin, revMax, callback) {
		this._getEventsByRevisionAsync(query, revMin, revMax)
			.then(resolvify(callback)).catch(rejectify(callback));
  },

  getUndispatchedEvents: function (query, callback) {
    var findStatement = {
      dispatched: false
    };

    if (query && query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query && query.context) {
      findStatement.context = query.context;
    }

    if (query && query.aggregateId) {
      findStatement.aggregateId = query.aggregateId;
    }

    this.events.find(findStatement).sort({
        commitStamp: 1, 
        streamRevision: 1,
        commitSequence: 1,
    }).toArray()
        .then(resolvify(callback))
        .catch(callback);
  },

  setEventToDispatched: function (id, callback) {
    var updateCommand = { '$unset' : { 'dispatched': null } };
    this.events.updateOne({'_id' : id}, updateCommand)
			.then(resolvify(callback)).catch(callback);
  },

  addSnapshot: function(snap, callback) {
    if (!snap.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    snap._id = snap.id;
    this.snapshots.insertOne(snap).then(resolvify(callback)).catch(callback);
  },

  cleanSnapshots: function (query, callback) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement = {
      aggregateId: query.aggregateId
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    this.snapshots.find(findStatement).sort({
        revision: -1,
        version: -1,
        commitStamp: -1,
      })
      .skip(this.options.maxSnapshotsCount)
      .toArray()
			.then(removeElements(this.snapshots, callback))
			.catch(rejectify(callback));
  },

  getSnapshot: function (query, revMax, callback) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement = {
      aggregateId: query.aggregateId
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    if (revMax > -1) {
      findStatement.revision = { '$lte': revMax };
    }

    this.snapshots.find(findStatement).sort({
        revision: -1,
        version: -1,
        commitStamp: -1,
    }).limit(1)
      .toArray()
      .then((res) => {
        return callback(null, res[0]);
      })
      .catch(callback);
  },

	_removeTransactionsAsync: async function (evt) {
    if (!evt.aggregateId)
      throw new Error('aggregateId not defined!');

    var findStatement = { aggregateId: evt.aggregateId };

    if (evt.aggregate) {
      findStatement.aggregate = evt.aggregate;
    }

    if (evt.context) {
      findStatement.context = evt.context;
    }

    // the following is usually unnecessary
    await this.transactions.deleteMany(findStatement);
  },

  removeTransactions: function (evt, callback) {
		this._removeTransactionsAsync(evt)
			.then(resolvify(callback))
			.catch(rejectify(callback))
  },

	_getPendingTransactionsAsync: async function () {
    const txs = await this.transactions.find({}).toArray();
		if (txs.length === 0)
			return txs;

		var goodTxs = [];

		for (const tx of txs) {
			var findStatement = { commitId: tx._id, aggregateId: tx.aggregateId };

			if (tx.aggregate) {
				findStatement.aggregate = tx.aggregate;
			}

			if (tx.context) {
				findStatement.context = tx.context;
			}

			const evt = await this.events.findOne(findStatement);
			if (evt) {
				goodTxs.push(evt);
				continue;
			}
				
			await this.transactions.deleteOne({ _id: tx._id });
		}

		return goodTxs;
  },

  getPendingTransactions: function (callback) {
		this._getPendingTransactionsAsync()
			.then(resolvify(callback)).catch(rejectify(callback));
  },

  getLastEvent: function (query, callback) {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement = { aggregateId: query.aggregateId };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    this.events.find(findStatement).sort({
        commitStamp: -1,
        streamRevision: -1,
        commitSequence: -1,
    }).limit(1)
      .toArray()
      .then((res) => {
        return callback(null, res[0]);
      })
      .catch(callback);
  },

	_repairFailedTransactionAsync: async function(lastEvt) {
    const tx = await this.transactions.findOne({ _id: lastEvt.commitId });
		if (!tx)
			throw new Error('missing tx entry for aggregate ' + lastEvt.aggregateId);

		var missingEvts = tx.events.slice(tx.events.length - lastEvt.restInCommitStream);

		await this.events.insertMany(missingEvts);
		this._removeTransactionsAsync(lastEvt);
	},

  repairFailedTransaction: function (lastEvt, callback) {
		this._repairFailedTransactionAsync(lastEvt)
			.then(resolvify(callback))
			.catch(rejectify(callback));
  }

});

function removeElements(collection, callback) {
  return async function (elements) {
		try {
			await collection.deleteMany({_id: { $in: elements.map(el => el._id) }});
			callback(null, elements.length);
		} catch (err) {
			callback(err, elements.length);
		}
  }
}

module.exports = Mongo;
