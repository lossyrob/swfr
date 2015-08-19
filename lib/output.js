"use strict";

var assert = require("assert"),
    os = require("os"),
    path = require("path"),
    url = require("url"),
    util = require("util");

var AWS = require("aws-sdk"),
    debug = require("debug"),
    fse = require("fs-extra"),
    holdtime = require("holdtime"),
    mkdirp = require("mkdirp"),
    Promise = require("bluebird"),
    s3UploadStream = require("s3-upload-stream"),
    tmp = require("tmp"),
    _ = require("underscore");

var log = debug("swfr:upload");

var ensureDir = Promise.promisify(fse.ensureDir);
var copy = Promise.promisify(fse.copy);

tmp.setGracefulCleanup();

var getContentType = function(extension) {
  switch (extension.toLowerCase()) {
  case ".tif":
  case ".tiff":
    return "image/tiff";

  case ".vrt":
    return "application/xml";

  default:
    return "application/octet-stream";
  }
};

var s3Upload = Promise.promisify(function(sourcePath, outputUri, done) {
  AWS.config.update({
    region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
  });

  var s3Stream = s3UploadStream(new AWS.S3());

  var outputURI = url.parse(outputUri),
      extension = path.extname(outputUri);

  var uploadData = {
    Bucket: outputURI.hostname,
    Key: outputURI.pathname.slice(1),
    ACL: "public-read", // TODO hard-coded
    ContentType: getContentType(extension)
  };

  log(util.format("S3 UPLOADING:\n%s", JSON.stringify(uploadData, null, 2)));

  var upload = s3Stream.upload(uploadData);

  var cleanup = holdtime(function() {
    log("upload: %dms", arguments[arguments.length - 1]);
  });

  upload.on("error", cleanup);
  upload.on("uploaded", cleanup);

  upload.on("error", function(err) {
    return done(new Error(err));
  });

  // cleanup can't be used, as it would propagate the info as an error
  upload.on("uploaded", function(info) {
    return done(null, outputUri);
  });

  return fse.createReadStream(sourcePath).pipe(upload);
});

var singleS3Output = function(outputUri, done, fn) {
  var extension = path.extname(outputUri);

  return tmp.tmpName({
    postfix: extension
  }, function(err, localOutputPath) {
    if (err) {
      return done(err);
    }

    return fn(null, localOutputPath, function(err, save) {
      if (err) {
        // wrapped function failed; propagate the error
        return done(err);
      }

      if (save == null) {
        return done(new Error("Activity must create output in order to upload to s3!"));
      }

      return s3Upload(localOutputPath, outputUri)
        .then(function(path) {
          return done(null, outputUri);
        })
        .catch(function(err) {
          return done(err);
        });
    });
  });
};


module.exports.singleOutput = function(outputUri, done, fn) {
  var uri = url.parse(outputUri);

  if (uri.protocol === "s3:") {
    return singleS3Output(outputUri, done, fn);
  }

  // Local output

  var d = path.dirname(outputUri);
  return mkdirp(d, function (err) {
    if (err) {
      return done(err);
    }

    return fn(null, outputUri, function(err, save) {
      if (err) {
        // wrapped function failed; propagate the error
        return done(err);
      }

      return done(null, outputUri);
    });
  });
};

module.exports.multiOutput = function(outputs, done, fn) {
  assert.ok(Array.isArray(outputs), "resample: targetExtent must be an array");

  return tmp.dir({ unsafeCleanup: true}, function(err, workingDir) {
    if (err) {
      return done(err);
    }

    return fn(null, workingDir, function(err, localOutputs) {
      if (err) {
        return done.apply(null, arguments);
      }

      assert.ok(outputs.length === localOutputs.length,
                util.format("Length of outputs URIs (%d) must be equal to length of local outputs (%d)", outputs.length, localOutputs.length));

      return Promise
        .resolve(_.zip(localOutputs, outputs))
        .map(function(sourceTarget) {
          var source = sourceTarget[0];
          var target = sourceTarget[1];

          var uri = url.parse(target);

          if (uri.protocol === "s3:") {
            return s3Upload(source, target);
          } else {
            var d = path.dirname(target);
            return ensureDir(d)
              .then(function(d) {
                return copy(source, target);
              });
          }
        }, { concurrency: os.cpus().length })
        .then(function(outputs) {
          return done(null, outputs);
        })
        .catch(function(err) {
          return done(err);
        });
    });
  });
};
