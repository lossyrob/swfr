"use strict";

var util = require("util");

var AWS = require("aws-sdk"),
    debug = require("debug"),
    env = require("require-env");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var log = debug("swfr:persister");

var dynamodb = new AWS.DynamoDB();

var PROTOCOL = "dynamodb:",
    TABLE_NAME = env.require("AWS_DYNAMODB_TABLE");

var makeURI = function(key) {
  return util.format("%s//%s/%s", PROTOCOL, TABLE_NAME, key);
};

// TODO add a function that determines whether a given persister is appropriate
module.exports = {
  // Assumptions:
  // key attribute is named "key" and is a String
  // "result" attribute exists, is a String, and contains JSON
  load: function(uri, callback) {
    var tableName = uri.hostname,
        key = uri.pathname.slice(1);
    log("Reading result from table %s key %s", tableName, key);
    return dynamodb.getItem({
      TableName: tableName,
      Key: {
        key: {
          S: key
        }
      },
      ConsistentRead: true
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      try {
        return callback(null, JSON.parse(data.Item.result.S));
      } catch (err) {
        return callback(err);
      }
    });
  },

  makeURI: makeURI,

  protocol: PROTOCOL,

  save: function(key, payload, callback) {
    log("Writing result to table %s key %s", TABLE_NAME, key);
    return dynamodb.putItem({
      TableName: TABLE_NAME,
      Item: {
        key: {
          S: key
        },
        result: {
          S: JSON.stringify(payload)
        },
        createdAt: {
          S: new Date().toISOString()
        }
      }
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      return callback(null, {
        uri: makeURI(key)
      });
    });
  }
};
