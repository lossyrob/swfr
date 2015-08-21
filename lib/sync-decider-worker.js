"use strict";

var os = require("os"),
    stream = require("stream"),
    util = require("util");

var Promise = require("bluebird");

var SyncDeciderContext = require("./sync-decider-context");

var SyncDeciderWorker = function(fn, activities) {
  stream.Writable.call(this, {
    objectMode: true,
    highWaterMark: 1 // limit the number of buffered tasks
  });

  this._write = function(task, encoding, callback) {
    // pass the input to the function and provide the rest as the context
    var context = new SyncDeciderContext(task, activities),
        chain = Promise.bind(context),
        input = task.input;

    // apply a hard cap to .map concurrency to the number of cores.
    var _map = chain.map;

    chain.map = function(fn, options) {
      options = options || {};
      options.concurrency = Math.min(options.concurrency, os.cpus().length) || os.cpus().length;

      return _map.apply(this, fn, options);
    };


    Promise
      .bind(context)
      .then(fn.bind(context, chain, input)) // partially apply the worker fn w/ the input
      .catch(function(err) {
        console.warn(err.stack);
      })
      .finally(function() {
        if (this.result) {
          console.log("Output:", this.result);
        }

        return callback();
      });
  };
};

util.inherits(SyncDeciderWorker, stream.Writable);

module.exports = SyncDeciderWorker;
