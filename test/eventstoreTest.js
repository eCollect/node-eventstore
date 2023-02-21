var expect = require('expect.js'),
  eventstore = require('../'),
  InMemory = require('../lib/databases/inmemory'),
  Base = require('../lib/base');

describe('eventstore', function () {

  it('it should be a function', function () {

    expect(eventstore).to.be.a('function');

  });

  it('it should exposed the Base for the Store implementation', function () {

    expect(eventstore.Store).to.eql(Base);

  });

  describe('calling that function', function() {

    describe('without options', function () {

      it('it should return as expected', function () {

        var es = eventstore();
        expect(es).to.be.a('object');
        expect(es.useEventPublisher).to.be.a('function');
        expect(es.init).to.be.a('function');
        expect(es.streamEvents).to.be.a('function');
        expect(es.streamEventsSince).to.be.a('function');
        expect(es.streamEventsByRevision).to.be.a('function');
        expect(es.getEvents).to.be.a('function');
        expect(es.getEventsSince).to.be.a('function');
        expect(es.getEventsByRevision).to.be.a('function');
        expect(es.getEventStream).to.be.a('function');
        expect(es.getFromSnapshot).to.be.a('function');
        expect(es.createSnapshot).to.be.a('function');
        expect(es.commit).to.be.a('function');
        expect(es.getUndispatchedEvents).to.be.a('function');
        expect(es.setEventToDispatched).to.be.a('function');
        expect(es.getNewId).to.be.a('function');

        expect(es.store).to.be.a(InMemory);

      });

    });

    describe('with options of a non existing db implementation', function () {

      it('it should throw an error', function () {

        expect(function () {
          eventstore({ type: 'strangeDb' });
        }).to.throwError();

      });

    });

    describe('with options of an own db implementation', function () {

      it('it should return with the an instance of that implementation', function () {

        var es = eventstore({ type: InMemory });
        expect(es).to.be.a('object');
        expect(es.useEventPublisher).to.be.a('function');
        expect(es.init).to.be.a('function');
        expect(es.streamEvents).to.be.a('function');
        expect(es.getEvents).to.be.a('function');
        expect(es.getEventsByRevision).to.be.a('function');
        expect(es.getEventStream).to.be.a('function');
        expect(es.getFromSnapshot).to.be.a('function');
        expect(es.createSnapshot).to.be.a('function');
        expect(es.commit).to.be.a('function');
        expect(es.getUndispatchedEvents).to.be.a('function');
        expect(es.setEventToDispatched).to.be.a('function');
        expect(es.getNewId).to.be.a('function');

        expect(es.store).to.be.a(InMemory);

      });

    });

    describe('and checking the api function by calling', function () {

      describe('getEvents', function () {

        var es = eventstore(),
          orgFunc = es.store.getEvents;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.getEvents = orgFunc;
        });

        describe('with nice arguments', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              skip: 2,
              limit: 32,
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.eql(given.query);
              expect(skip).to.eql(given.skip);
              expect(limit).to.eql(given.limit);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.query, given.skip, given.limit, given.callback);

          });

        });

        describe('with only the callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.be.an('object');
              expect(query).to.empty();
              expect(skip).to.eql(0);
              expect(limit).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.callback);

          });

        });

        describe('with query and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.eql(given.query);
              expect(skip).to.eql(0);
              expect(limit).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.query, given.callback);

          });

        });

        describe('with skip and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              skip: 3,
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.be.an('object');
              expect(query).to.empty();
              expect(skip).to.eql(given.skip);
              expect(limit).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.skip, given.callback);

          });

        });

        describe('with query, skip and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              skip: 3,
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.eql(given.query);
              expect(skip).to.eql(given.skip);
              expect(limit).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.query, given.skip, given.callback);

          });

        });

        describe('with skip, limit and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              skip: 3,
              limit: 50,
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.be.an('object');
              expect(query).to.empty();
              expect(skip).to.eql(given.skip);
              expect(limit).to.eql(given.limit);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.skip, given.limit, given.callback);

          });

        });

        describe('with query as string,  skip, limit and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: 'myAggId',
              skip: 3,
              limit: 50,
              callback: function () {}
            };

            es.store.getEvents = function (query, skip, limit, callback) {
              expect(query).to.be.an('object');
              expect(query.aggregateId).to.eql('myAggId');
              expect(skip).to.eql(given.skip);
              expect(limit).to.eql(given.limit);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEvents(given.query, given.skip, given.limit, given.callback);

          });

        });

      });

      describe('getEventsByRevision', function () {

        var es = eventstore(),
          orgFunc = es.store.getEventsByRevision;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.getEventsByRevision = orgFunc;
        });

        describe('with nice arguments', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              revMin: 2,
              revMax: 32,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(given.revMax);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventsByRevision(given.query, given.revMin, given.revMax, given.callback);

          });

        });

        describe('with query and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(0);
              expect(revMax).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventsByRevision(given.query, given.callback);

          });

        });

        describe('with query, revMin and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              revMin: 2,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventsByRevision(given.query, given.revMin, given.callback);

          });

        });

        describe('with query as string, revMin, revMax and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: 'myAggId',
              revMin: 2,
              revMax: 4,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.be.an('object');
              expect(query.aggregateId).to.eql('myAggId');
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(given.revMax);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventsByRevision(given.query, given.revMin, given.revMax, given.callback);

          });

        });

        describe('with wrong query', function () {

          it('it should pass them correctly', function (done) {

            es.getEventsByRevision(123, 3, 100, function (err) {
              expect(err.message).to.match(/aggregateId/);
              done();
            });

          });

        });

      });

      describe('getEventStream', function () {

        var es = eventstore(),
          orgFunc = es.store.getEventsByRevision;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.getEventsByRevision = orgFunc;
        });

        describe('with nice arguments', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              revMin: 2,
              revMax: 32,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(given.revMax);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventStream(given.query, given.revMin, given.revMax, given.callback);

          });

        });

        describe('with query and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(0);
              expect(revMax).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventStream(given.query, given.callback);

          });

        });

        describe('with query, revMin and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              revMin: 2,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventStream(given.query, given.revMin, given.callback);

          });

        });

        describe('with query as string, revMin, revMax and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: 'myAggId',
              revMin: 2,
              revMax: 4,
              callback: function () {}
            };

            es.store.getEventsByRevision = function (query, revMin, revMax, callback) {
              expect(query).to.be.an('object');
              expect(query.aggregateId).to.eql('myAggId');
              expect(revMin).to.eql(given.revMin);
              expect(revMax).to.eql(given.revMax);
              expect(callback).to.be.a('function');

              done();
            };

            es.getEventStream(given.query, given.revMin, given.revMax, given.callback);

          });

        });

        describe('with wrong query', function () {

          it('it should pass them correctly', function (done) {

            es.getEventStream(123, 3, 100, function (err) {
              expect(err.message).to.match(/aggregateId/);
              done();
            });

          });

        });

      });

      describe('getFromSnapshot', function () {

        var es = eventstore(),
          orgFunc = es.store.getSnapshot;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.getSnapshot = orgFunc;
        });

        describe('with nice arguments', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              revMax: 32,
              callback: function () {
              }
            };

            es.store.getSnapshot = function (query, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMax).to.eql(given.revMax);
              expect(callback).to.be.a('function');

              done();
            };

            es.getFromSnapshot(given.query, given.revMax, given.callback);

          });

        });

        describe('with query and callback', function () {

          it('it should pass them correctly', function (done) {

            var given = {
              query: { aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' },
              callback: function () {
              }
            };

            es.store.getSnapshot = function (query, revMax, callback) {
              expect(query).to.eql(given.query);
              expect(revMax).to.eql(-1);
              expect(callback).to.be.a('function');

              done();
            };

            es.getFromSnapshot(given.query, given.callback);

          });

          describe('with query as string, revMax and callback', function () {

            it('it should pass them correctly', function (done) {

              var given = {
                query: 'myAggId',
                revMax: 31,
                callback: function () {
                }
              };

              es.store.getSnapshot = function (query, revMax, callback) {
                expect(query).to.be.an('object');
                expect(query.aggregateId).to.eql('myAggId');
                expect(revMax).to.eql(31);
                expect(callback).to.be.a('function');

                done();
              };

              es.getFromSnapshot(given.query, given.revMax, given.callback);

            });

          });

          describe('with wrong query', function () {

            it('it should pass them correctly', function (done) {

              es.getFromSnapshot(123, 100, function (err) {
                expect(err.message).to.match(/aggregateId/);
                done();
              });

            });

          });

        });

      });

      describe('createSnapshot', function () {

        var es = eventstore(),
          orgFunc = es.store.addSnapshot;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.addSnapshot = orgFunc;
        });

        describe('with nice arguments', function () {

          it('it should pass them correctly', function (done) {

            var obj = {
              aggregateId: 'myAggId',
              aggregate: 'myAgg',
              context: 'myCont',
              data: { snap: 'data' }
            };

            es.store.addSnapshot = function (snap, callback) {
              expect(snap.aggregateId).to.eql(obj.aggregateId);
              expect(snap.aggregate).to.eql(obj.aggregate);
              expect(snap.context).to.eql(obj.context);
              expect(snap.data).to.eql(obj.data);
              expect(callback).to.be.a('function');

              done();
            };

            es.createSnapshot(obj, function () {});

          });

        });

        describe('with streamId', function () {

          it('it should pass them correctly', function (done) {

            var obj = {
              streamId: 'myAggId',
              data: { snap: 'data' }
            };

            es.store.addSnapshot = function (snap, callback) {
              expect(snap.aggregateId).to.eql(obj.streamId);
              expect(snap.data).to.eql(obj.data);
              expect(callback).to.be.a('function');

              done();
            };

            es.createSnapshot(obj, function () {});

          });

        });

        describe('with wrong aggregateId', function () {

          it('it should pass them correctly', function (done) {

            var obj = {
              data: { snap: 'data' }
            };

            es.createSnapshot(obj, function (err) {
              expect(err.message).to.match(/aggregateId/);
              done();
            });

          });

        });

        describe('with wrong data', function () {

          it('it should pass them correctly', function (done) {

            var obj = {
              aggregateId: 'myAggId',
              aggregate: 'myAgg',
              context: 'myCont'
            };

            es.createSnapshot(obj, function (err) {
              expect(err.message).to.match(/data/);
              done();
            });

          });

        });

      });

      describe('cleanSnapshots', function () {

        var es = eventstore({
            maxSnapshotsCount: 5
          }),
          orgFunc = es.store.cleanSnapshots,
          addSnapshot = es.store.addSnapshot;

        before(function (done) {
          es.store.addSnapshot = function (snap, callback) {
            callback();
          };
          es.init(done);
        });

        after(function () {
          es.store.cleanSnapshots = orgFunc;
          es.store.addSnapshot = addSnapshot;
        });

        describe('with streamId', function () {

          it('it should pass them correctly', function (done) {

            var obj = {
              streamId: 'myAggId',
              aggregate: 'myAgg',
              context: 'myCont',
              data: { snap: 'data' }
            };

            es.store.cleanSnapshots = function (query, callback) {
              expect(query.aggregateId).to.eql(obj.streamId);
              expect(query.aggregate).to.eql(obj.aggregate);
              expect(query.context).to.eql(obj.context);
              expect(callback).to.be.a('function');
              callback();
            };

            es.createSnapshot(obj, done);
          });

        });

        describe('with options not activated', function () {

          before(function () {
            es.options.maxSnapshotsCount = 0;
          });

          it('it should not clean snapshots', function (done) {

            var obj = {
              streamId: 'myAggId',
              aggregate: 'myAgg',
              context: 'myCont',
              data: { snap: 'data' }
            };

            es.store.cleanSnapshots = function (query, callback) {
              callback(new Error('clean snapshots should not have been called'));
            };

            es.createSnapshot(obj, done);
          });

        });
      });

      describe('setEventToDispatched', function () {

        var es = eventstore(),
          orgFunc = es.store.setEventToDispatched;

        before(function (done) {
          es.init(done);
        });

        after(function () {
          es.store.setEventToDispatched = orgFunc;
        });

        describe('with an event', function () {

          it('it should pass it correctly', function (done) {

            var evt = {
              commitId: '1234'
            };

            es.store.setEventToDispatched = function (id, callback) {
              expect(id).to.eql(evt.id);
              expect(callback).to.be.a('function');

              done();
            };

            es.setEventToDispatched(evt, function () {
            });

          });

        });

        describe('with a commitId', function () {

          it('it should pass it correctly', function (done) {

            var evt = {
              commitId: '1234'
            };

            es.store.setEventToDispatched = function (id, callback) {
              expect(id).to.eql(evt.commitId);
              expect(callback).to.be.a('function');

              done();
            };

            es.setEventToDispatched(evt.commitId, function () {});

          });

        });

      });

    });

    describe('with options containing a type property with the value of', function () {

      var types = ['inmemory', 'tingodb', 'mongodb', 'redis'];
      var streamingApiTypes = ['mongodb'];
      var positionTypes = ['mongodb', 'inmemory'];

      types.forEach(function (type) {

        describe('"' + type + '"', function () {

          var es = null;

          var options = {};

          before(function () {
            if (type === 'redis') {
              options = {
                db: 3
              };
            }
            options.type = type;
          });

          describe('calling init without callback', function () {

            afterEach(function (done) {
              es.store.disconnect(done);
            });

            beforeEach(function () {
              es = eventstore(options);
            });

            it('it should emit connect', function (done) {

              es.init();
              es.once('connect', done);

            });

          });

          describe('calling init with callback', function () {

            afterEach(function (done) {
              es.store.disconnect(done);
            });

            beforeEach(function () {
              es = eventstore(options);
            });

            it('it should callback successfully', function (done) {

              es.init(function(err) {
                expect(err).not.to.be.ok();
                done();
              });

            });

          });

          describe('having initialized (connected)', function () {

            describe('calling disconnect on store', function () {

              beforeEach(function (done) {
                es = eventstore(options);
                es.init(done);
              });

              it('it should callback successfully', function (done) {

                es.store.disconnect(function (err) {
                  expect(err).not.to.be.ok();
                  done();
                });

              });

              it('it should emit disconnect', function (done) {

                es.once('disconnect', done);
                es.store.disconnect();

              });

            });

            describe('using the eventstore', function () {

              before(function (done) {
                es = eventstore(options);
                es.init(function(err) {
                  es.store.clear(done);
                });
              });

              after(function (done) {
                es.store.clear(done);
              });

              describe('calling getNewId', function () {

                it('it should callback with a new Id as string', function (done) {

                  es.getNewId(function (err, id) {
                    expect(err).not.to.be.ok();
                    expect(id).to.be.a('string');
                    done();
                  });

                });

              });

              describe('requesting a new eventstream', function () {

                describe('and committing some new events', function () {

                  it('it should work as expected', function (done) {

                    es.getEventStream({ aggregateId: 'myAggId', aggregate: 'myAgg', context: 'myCont' }, function (err, stream) {
                      expect(err).not.to.be.ok();

                      expect(stream.lastRevision).to.eql(-1);

                      stream.addEvents([{ one: 'event1' }, { two: 'event2' }, { three: 'event3' }]);

                      expect(stream.streamId).to.eql('myAggId');
                      expect(stream.uncommittedEvents.length).to.eql(3);
                      expect(stream.events.length).to.eql(0);
                      expect(stream.lastRevision).to.eql(-1);

                      stream.commit(function(err, str) {
                        expect(err).not.to.be.ok();
                        expect(str).to.eql(stream);

                        expect(str.uncommittedEvents.length).to.eql(0);
                        expect(str.events.length).to.eql(3);
                        expect(str.lastRevision).to.eql(2);

                        expect(str.events[0].commitSequence).to.eql(0);
                        expect(str.events[1].commitSequence).to.eql(1);
                        expect(str.events[2].commitSequence).to.eql(2);

                        expect(str.events[0].restInCommitStream).to.eql(2);
                        expect(str.events[1].restInCommitStream).to.eql(1);
                        expect(str.events[2].restInCommitStream).to.eql(0);

                        expect(str.eventsToDispatch.length).to.eql(3);

                        done();
                      });

                    });

                  });

                });

              });

              describe('requesting an existing eventstream', function () {

                describe('and committing some new events', function () {

                  before(function(done) {
                    es.getEventStream({ aggregateId: 'myAggId2', aggregate: 'myAgg', context: 'myCont' }, function (err, stream) {
                      expect(err).not.to.be.ok();

                      stream.addEvents([{ one: 'event1' }, { two: 'event2' }, { three: 'event3' }]);
                      stream.commit(done);
                    });
                  });

                  it('it should work as expected', function (done) {

                    es.getEventStream({ aggregateId: 'myAggId2', aggregate: 'myAgg', context: 'myCont' }, function (err, stream) {
                      expect(err).not.to.be.ok();

                      expect(stream.lastRevision).to.eql(2);

                      stream.addEvents([{ for: 'event4' }, { five: 'event5' }]);

                      expect(stream.streamId).to.eql('myAggId2');
                      expect(stream.uncommittedEvents.length).to.eql(2);
                      expect(stream.events.length).to.eql(3);
                      expect(stream.lastRevision).to.eql(2);

                      stream.commit(function(err, str) {
                        expect(err).not.to.be.ok();
                        expect(str).to.eql(stream);

                        expect(str.uncommittedEvents.length).to.eql(0);
                        expect(str.events.length).to.eql(5);
                        expect(str.lastRevision).to.eql(4);

                        expect(str.events[3].commitSequence).to.eql(0);
                        expect(str.events[4].commitSequence).to.eql(1);

                        expect(str.events[3].restInCommitStream).to.eql(1);
                        expect(str.events[4].restInCommitStream).to.eql(0);

                        expect(str.eventsToDispatch.length).to.eql(2);

                        done();
                      });

                    });

                  });

                  it('it should be able to retrieve them', function (done) {

                    es.getEvents({ aggregateId: 'myAggId2', aggregate: 'myAgg', context: 'myCont' }, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(5);

                      done();
                    });

                  });

                  it('it should be able to retrieve by context', function (done) {

                    es.getEvents({context: 'myCont' }, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(8);

                      done();
                    });

                  });

                });

              });

              describe('requesting existing events and using next function', function () {

                describe('and committing some new events', function () {

                  it('it should work as expected', function (done) {

                    es.getEvents({ aggregate: 'myAgg', context: 'myCont' }, 0, 3, function (err, evts) {
                      expect(err).not.to.be.ok();

                      expect(evts.length).to.eql(3);

                      expect(evts.next).to.be.a('function');

                      evts.next(function (err, nextEvts) {
                        expect(err).not.to.be.ok();

                        expect(nextEvts.length).to.eql(3);

                        expect(nextEvts.next).to.be.a('function');

                        nextEvts.next(function (err, nextNextEvts) {
                          expect(err).not.to.be.ok();

                          expect(nextNextEvts.length).to.eql(2);

                          expect(nextNextEvts.next).to.be.a('function');

                          done();
                        });
                      });

                    });

                  });

                });

              });

              describe('requesting all existing events, without query argument and using next function', function () {

                describe('and committing some new events', function () {

                  it('it should work as expected', function (done) {

                    es.getEvents(0, 3, function (err, evts) {
                      expect(err).not.to.be.ok();

                      expect(evts.length).to.eql(3);

                      expect(evts.next).to.be.a('function');

                      evts.next(function (err, nextEvts) {
                        expect(err).not.to.be.ok();

                        expect(nextEvts.length).to.eql(3);

                        expect(nextEvts.next).to.be.a('function');

                        nextEvts.next(function (err, nextNextEvts) {
                          expect(err).not.to.be.ok();

                          expect(nextNextEvts.length).to.eql(2);

                          expect(nextNextEvts.next).to.be.a('function');

                          done();
                        });
                      });

                    });

                  });

                });

              });

              describe('requesting existing events since a date and using next function', function () {

                describe('and committing some new events', function () {

                  it('it should work as expected', function (done) {

                    es.getEventsSince(new Date(2000, 1, 1), 0, 3, function (err, evts) {
                      expect(err).not.to.be.ok();

                      expect(evts.length).to.eql(3);

                      expect(evts.next).to.be.a('function');

                      evts.next(function (err, nextEvts) {
                        expect(err).not.to.be.ok();

                        expect(nextEvts.length).to.eql(3);

                        expect(nextEvts.next).to.be.a('function');

                        nextEvts.next(function (err, nextNextEvts) {
                          expect(err).not.to.be.ok();

                          expect(nextNextEvts.length).to.eql(2);

                          expect(nextNextEvts.next).to.be.a('function');

                          done();
                        });
                      });

                    });

                  });

                });

              });

              describe('requesting all undispatched events', function () {

                it('it should return the correct events', function (done) {

                  es.getUndispatchedEvents(function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(8);

                    done();
                  });

                });

              });

              describe('requesting all undispatched events by streamId', function () {

                it('it should return the correct events', function (done) {

                  es.getUndispatchedEvents('myAggId2', function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(5);

                    done();
                  });

                });

              });

              describe('requesting all undispatched events by query', function () {

                describe('aggregateId', function () {

                  it('it should return the correct events', function (done) {

                    es.getUndispatchedEvents({ aggregateId: 'myAggId' }, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(3);

                      done();
                    });

                  });

                });

                describe('aggregate', function () {

                  it('it should return the correct events', function (done) {

                    es.getUndispatchedEvents({ aggregate: 'myAgg' }, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(8);

                      done();
                    });

                  });

                });

                describe('context', function () {

                  it('it should return the correct events', function (done) {

                    es.getUndispatchedEvents({ context: 'myCont' }, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(8);

                      done();
                    });

                  });

                });

              });

              describe('setting an event to dispatched', function () {

                it('it should work correctly', function (done) {

                  es.getUndispatchedEvents(function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(8);

                    es.setEventToDispatched(evts[0], function (err) {
                      expect(err).not.to.be.ok();

                      es.getUndispatchedEvents(function (err, evts) {
                        expect(err).not.to.be.ok();
                        expect(evts.length).to.eql(7);

                        done();
                      });
                    });

                  });

                });

              });

              describe('creating a snapshot', function () {

                it('it should callback without error', function (done) {

                  es.getEventStream({ aggregateId: 'myAggIdOfSnap', aggregate: 'myAgg', context: 'myCont' }, function (err, stream) {
                    expect(err).not.to.be.ok();

                    expect(stream.lastRevision).to.eql(-1);

                    stream.addEvents([{ oneSnap: 'event1' }, { twoSnap: 'event2' }, { threeSnap: 'event3' }]);

                    expect(stream.streamId).to.eql('myAggIdOfSnap');
                    expect(stream.uncommittedEvents.length).to.eql(3);
                    expect(stream.events.length).to.eql(0);
                    expect(stream.lastRevision).to.eql(-1);

                    stream.commit(function(err, str) {
                      expect(err).not.to.be.ok();
                      expect(str).to.eql(stream);

                      expect(str.uncommittedEvents.length).to.eql(0);
                      expect(str.events.length).to.eql(3);
                      expect(str.lastRevision).to.eql(2);

                      expect(str.events[0].commitSequence).to.eql(0);
                      expect(str.events[1].commitSequence).to.eql(1);
                      expect(str.events[2].commitSequence).to.eql(2);

                      expect(str.events[0].restInCommitStream).to.eql(2);
                      expect(str.events[1].restInCommitStream).to.eql(1);
                      expect(str.events[2].restInCommitStream).to.eql(0);

                      expect(str.eventsToDispatch.length).to.eql(3);

                      es.createSnapshot({
                        aggregateId: stream.aggregateId,
                        aggregate: stream.aggregate,
                        context: stream.context,
                        revision: stream.lastRevision,
                        version: 1,
                        data: { my: 'snap' }
                      }, function (err) {
                        expect(err).not.to.be.ok();

                        stream.addEvent({ fourSnap: 'event4' });

                        stream.commit(function(err, str) {
                          expect(err).not.to.be.ok();
                          expect(str).to.eql(stream);

                          expect(str.uncommittedEvents.length).to.eql(0);
                          expect(str.events.length).to.eql(4);
                          expect(str.lastRevision).to.eql(3);

                          expect(str.eventsToDispatch.length).to.eql(1);

                          done();
                        });

                      });

                    });

                  });

                });

                it('it should callback without error with no additional events', function (done) {

                  es.getEventStream({ aggregateId: 'myAggIdOfSnap2', aggregate: 'myAgg', context: 'myCont' }, function (err, stream) {
                    expect(err).not.to.be.ok();

                    expect(stream.lastRevision).to.eql(-1);

                    stream.addEvents([{ oneSnap: 'event1' }, { twoSnap: 'event2' }, { threeSnap: 'event3' }]);

                    expect(stream.streamId).to.eql('myAggIdOfSnap2');
                    expect(stream.uncommittedEvents.length).to.eql(3);
                    expect(stream.events.length).to.eql(0);
                    expect(stream.lastRevision).to.eql(-1);

                    stream.commit(function(err, str) {
                      expect(err).not.to.be.ok();
                      expect(str).to.eql(stream);

                      expect(str.uncommittedEvents.length).to.eql(0);
                      expect(str.events.length).to.eql(3);
                      expect(str.lastRevision).to.eql(2);

                      expect(str.events[0].commitSequence).to.eql(0);
                      expect(str.events[1].commitSequence).to.eql(1);
                      expect(str.events[2].commitSequence).to.eql(2);

                      expect(str.events[0].restInCommitStream).to.eql(2);
                      expect(str.events[1].restInCommitStream).to.eql(1);
                      expect(str.events[2].restInCommitStream).to.eql(0);

                      expect(str.eventsToDispatch.length).to.eql(3);

                      es.createSnapshot({
                        aggregateId: stream.aggregateId,
                        aggregate: stream.aggregate,
                        context: stream.context,
                        revision: stream.lastRevision,
                        version: 1,
                        data: { my: 'snap' }
                      }, function (err) {
                        expect(err).not.to.be.ok();
                        done();
                      });
                    });

                  });

                });

                describe('and call getFromSnapshot', function () {

                  it('it should retrieve it and the missing events', function (done) {

                    es.getFromSnapshot({ aggregateId: 'myAggIdOfSnap' }, -1, function (err, snap, stream) {
                      expect(err).not.to.be.ok();

                      expect(snap.aggregateId).to.eql('myAggIdOfSnap');
                      expect(snap.revision).to.eql(2);
                      expect(snap.version).to.eql(1);
                      expect(snap.data.my).to.eql('snap');

                      expect(stream.lastRevision).to.eql(3);

                      done();
                    });

                  });

                  it('it should set the lastRevision of an empty event stream to the snapshot revision', function(done) {

                    es.getFromSnapshot({ aggregateId: 'myAggIdOfSnap2' }, -1, function (err, snap, stream) {
                      expect(err).not.to.be.ok();

                      expect(stream.lastRevision).to.eql(snap.revision);

                      done();
                    });

                  });

                });

              });

              if (streamingApiTypes.indexOf(type) !== -1) {
                describe('streaming api', function () {
                  describe('streaming existing events', function () {                
                    describe('and committing some new events', function () {
                      it('it should work as expected', function (done) {

                        var evts = [];
                        var stream = es.streamEvents({ aggregate: 'myAgg', context: 'myCont' }, 0, 3);
                        stream.on('data', function (e) {
                          evts.push(e);
                        });
                        stream.on('end', function(){
                          expect(evts.length).to.eql(3);
                          done();
                        });

                      });

                    });

                  });
                  describe('streaming all existing events, without query argument', function () {
                    describe('and committing some new events', function () {
                      it('it should work as expected', function (done) {

                        var evts = [];
                        var stream =  es.streamEvents(0, 3);
                        stream.on('data', function (e) {
                          evts.push(e);
                        });
                        stream.on('end', function(){
                          expect(evts.length).to.eql(3);
                          done();
                        });

                      });
                    });
                  });
      
                  describe('requesting existing events since a date', function () {
                    describe('and committing some new events', function () {
                      it('it should work as expected', function (done) {
                        var evts = [];
                        var stream =  es.streamEventsSince(new Date(2000, 1, 1), 0, 3);
                        stream.on('data', function (e) {
                          evts.push(e);
                        });
                        stream.on('end', function(){
                          expect(evts.length).to.eql(3);
                          done();
                        });
                      });
                    });
                  });
                  describe('requesting existing events by revision', function () {
                    describe('and committing some new events', function () {
                      it('it should work as expected', function (done) {
                        var evts = [];
                        var stream =  es.streamEventsByRevision('myAggId2', 0, 3);
                        stream.on('data', function (e) {
                          evts.push(e);
                        });
                        stream.on('end', function(){
                          expect(evts.length).to.eql(3);
                          done();
                        });
                      });
                    });
                  });

                });                    
              }

              if (positionTypes.indexOf(type) !== -1) {
                describe('setting event position option', function() {
                  beforeEach(function (done) {
                    es = eventstore({
                      type: type,
                      positionsCollectionName: 'positions',
                      trackPosition: true,
                    });
                    es.defineEventMappings({ position: 'head.position' });
                    es.init(function(err) {
                      es.store.clear(done);
                    });
                  });
    
                  afterEach(function (done) {
                    es.store.clear(done);
                  });

                  it('it should save the event with position', function(done) {
                    es.getEventStream('streamIdWithPosition', function (err, stream) {
                      expect(err).not.to.be.ok();
                      stream.addEvent({ one: 'event' });
                      stream.addEvent({ one: 'event-other' });
          
                      stream.commit(function (err, st) {
                        expect(err).not.to.be.ok();
          
                        expect(st.events.length).to.eql(2);
                        expect(st.events[0].position).to.eql(1);
                        expect(st.events[1].position).to.eql(2);
          
                        done();
                      });
                    });
                  });

                  it('it should map position to payload', function(done) {
                    es.getEventStream('streamIdWithPosition', function (err, stream) {
                      expect(err).not.to.be.ok();
                      stream.addEvent({ one: 'event' });
                      stream.addEvent({ one: 'event-other' });
          
                      stream.commit(function (err, st) {
                        expect(err).not.to.be.ok();
          
                        expect(st.events.length).to.eql(2);
                        expect(st.events[0].payload.head.position).to.eql(1);
                        expect(st.events[1].payload.head.position).to.eql(2);
          
                        done();
                      });
                    });
                  });

                });
              }
            });

          });

        });

      });

    });

    describe('and defining the commitStamp option', function () {

      it('it should save the commitStamp correctly', function (done) {

        var es = eventstore();
        es.defineEventMappings({ commitStamp: 'head.date' });
        es.init(function (err) {
          expect(err).not.to.be.ok();

          es.getEventStream('streamIdWithDate', function (err, stream) {
            stream.addEvent({ one: 'event' });

            stream.commit(function (err, st) {
              expect(err).not.to.be.ok();

              expect(st.events.length).to.eql(1);
              expect(st.events[0].payload.head.date).to.eql(st.events[0].commitStamp);

              done();
            });
          });
        });

      });

    });

    describe('and not defining the commitStamp option', function () {

      it('it should not save the commitStamp', function (done) {

        var es = eventstore({});
        es.init(function (err) {
          expect(err).not.to.be.ok();

          es.getEventStream('streamIdWithoutDate', function (err, stream) {
            stream.addEvent({ one: 'event' });

            stream.commit(function (err, st) {
              expect(err).not.to.be.ok();

              expect(st.events.length).to.eql(1);
              expect(st.events[0].payload.date).not.to.be.ok();
              expect(st.events[0].payload.head).not.to.be.ok();

              done();
            });
          });
        });

      });

    });

    describe('and defining the streamRevision option', function () {

      it('it should save the streamRevision correctly', function (done) {

        var es = eventstore();
        es.defineEventMappings({ streamRevision: 'version' });
        es.init(function (err) {
          expect(err).not.to.be.ok();

          es.getEventStream('streamIdWithDate', function (err, stream) {
            stream.addEvent({ one: 'event' });

            stream.commit(function (err, st) {
              expect(err).not.to.be.ok();

              expect(st.events.length).to.eql(1);
              expect(st.events[0].payload.version).to.eql(st.events[0].streamRevision);

              done();
            });
          });
        });

      });

    });

    describe('and defining a publisher function in a synchronous way', function () {

      it('it should initialize an eventDispatcher', function (done) {

        function publish (evt) {}
        var es = eventstore();
        es.useEventPublisher(publish);
        es.init(function (err) {
          expect(err).not.to.be.ok();
          expect(es.publisher).to.be.ok();
          done();
        });

      });

      describe('when committing a new event', function () {

        it('it should publish a new event', function (done) {

          function publish (evt) {
            expect(evt.one).to.eql('event');
            done();
          }

          var es = eventstore();
          es.useEventPublisher(publish);
          es.init(function (err) {
            expect(err).not.to.be.ok();

            es.getEventStream('streamId', function (err, stream) {
              stream.addEvent({ one: 'event' });

              stream.commit(function (err) {
                expect(err).not.to.be.ok();
              });
            });
          });

        });

      });

    });

    describe('and defining a publisher function in an asynchronous way', function () {

      it('it should initialize an eventDispatcher', function (done) {

        function publish (evt, callback) {callback();}
        var es = eventstore();
        es.useEventPublisher(publish);
        es.init(function (err) {
          expect(err).not.to.be.ok();
          expect(es.publisher).to.be.ok();
          done();
        });

      });

      describe('when committing a new event', function () {

        it('it should publish a new event', function (done) {

          function publish (evt, callback) {
            expect(evt.one).to.eql('event');
            callback();
            done();
          }

          var es = eventstore();
          es.useEventPublisher(publish);
          es.init(function (err) {
            expect(err).not.to.be.ok();

            es.getEventStream('streamId', function (err, stream) {
              stream.addEvent({ one: 'event' });

              stream.commit(function (err) {
                expect(err).not.to.be.ok();
              });
            });
          });

        });

      });

    });

    describe('and not defining a publisher function', function () {

      it('it should not initialize an eventDispatcher', function (done) {

        var es = eventstore();
        es.init(function (err) {
          expect(err).not.to.be.ok();
          expect(es.publisher).not.to.be.ok();
          done();
        });

      });

    });

  });

});
