"use strict";

var fs = require("fs"),
    path = require("path"),
    url = require("url"),
    util = require("util");

var AWS = require("aws-sdk"),
    debug = require("debug"),
    holdtime = require("holdtime"),
    s3UploadStream = require("s3-upload-stream"),
    tmp = require("tmp");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var log = debug("swfr:upload"),
    s3Stream = s3UploadStream(new AWS.S3());

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

/**
 * @param output string S3 output URI.
 * @param done function Completion callback.
 * @param callback function Wrapped function.
 */
module.exports = function(output, done, callback) {
  var outputURI = url.parse(output),
      extension = path.extname(output);

  return tmp.tmpName({
    postfix: extension
  }, function(err, outputFilename) {
    return callback(err, outputFilename, function(err, outputs) {
      if (err) {
        // wrapped function failed; propagate the error
        return done(err);
      }

      if (save == null) {
        return done(new Error("Activity must create output in order to upload to s3!"));
      }

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

        fs.unlink(outputFilename, function() {});
      });

      upload.on("error", cleanup);
      upload.on("uploaded", cleanup);

      upload.on("error", function(err) {
        return done(new Error(err));
      });

      // cleanup can't be used, as it would propagate the info as an error
      upload.on("uploaded", function(info) {
        return done(null, output);
      });

      return fs.createReadStream(outputFilename).pipe(upload);
    });
  });
};
