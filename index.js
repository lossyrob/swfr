"use strict";

var activities = require("./lib/activities"),
    activity = require("./lib/activity"),
    decider = require("./lib/decider"),
    output = require("./lib/output"),
    shell = require("./lib/shell");

module.exports.activities = activities;
module.exports.activity = activity;
module.exports.decider = decider;
module.exports.singleOutput = output.singleOutput;
module.exports.multiOutput = output.multiOutput;
module.exports.shell = shell;
