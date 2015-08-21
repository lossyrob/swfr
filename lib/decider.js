"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk"),
    debug = require("debug");

var activities = require("./activities"),
    DeciderWorker = require("./decider-worker"),
    SyncDeciderWorker = require("./sync-decider-worker");

var log = debug("swfr:decider");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var swf = new AWS.SWF();

/**
 * Available options:
 * * domain - Workflow domain (required)
 * * taskList - Task list
 */
// TODO Distributor
module.exports = function(options, fn) {
  if (options.sync) {
    if(!options.activities) {
      assert.ok(options.activitiesFolder, "options.activities or options.activitiesFolder is required");
      options.activities = activities(options.activitiesFolder);
    }

    var worker = new EventEmitter();

    var source = new stream.PassThrough({
      objectMode: true
    });

    source.pipe(new SyncDeciderWorker(fn, options.activities));

    worker.cancel = function() {
      source.end();
    };

    worker.start = function(input) {
      source.write({
        input: input
      });
    };

    return worker;
  }

  assert.ok(options.domain, "options.domain is required");

  options.taskList = options.taskList || "defaultTaskList";

  var worker = new EventEmitter();

  var getEvents = function(events, nextPageToken, callback) {
    if (!nextPageToken) {
      return setImmediate(callback, null, events);
    }

    return swf.pollForDecisionTask({
      domain: options.domain,
      taskList: {
        name: options.taskList
      },
      identity: util.format("swfr@%s:%d", os.hostname(), process.pid),
      nextPageToken: nextPageToken
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      if (data.nextPageToken) {
        return getEvents(events.concat(data.events), data.nextPageToken, callback);
      }

      return callback(null, events.concat(data.events));
    });
  };

  var source = _(function(push, next) {
    // TODO note: activity types need to be registered in order for workflow
    // executions to not fail

    // TODO get events from an LRU cache (which means requesting events in
    // reverse order)
    // in batches of 100 (or whatever the page size is set to)
    log("POLLING FOR DECISIONS.. %s %s %s", AWS.config.region, options.domain, options.taskList);
    var poll = swf.pollForDecisionTask({
      domain: options.domain,
      taskList: {
        name: options.taskList
      },
      identity: util.format("swfr@%s:%d", os.hostname(), process.pid)
    }, function(err, data) {
      log("Got decision task!");
      if (err) {
        console.warn(err.stack);

        return next();
      }

      if (!data.taskToken) {
        return next();
      }

      return getEvents(data.events, data.nextPageToken, function(err, events) {
        if (err) {
          console.warn(err.stack);

          return next();
        }
        log("Deciding on %d events.", data.events.length);
        data.events = events;

        push(null, data);

        return next();
      });
    });

    // cancel requests when the stream ends so we're not hanging onto any
    // outstanding resources (swf.pollForDecisionTask waits 60s for messages
    // by default)

    var abort = poll.abort.bind(poll);

    source.on("end", abort);

    // clean up event listeners
    poll.on("complete", _.partial(source.removeListener.bind(source), "end", abort));
  });

  if (fn) {
    source.pipe(new DeciderWorker(fn));

    worker.cancel = function() {
      source.destroy();
    };
  }

  return worker;
};
