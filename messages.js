var protobuf = require('protocol-buffers')

module.exports = protobuf(`
  message Content {
    required bytes contentFeed = 1;
  }

  message Entry {
    message Data {
      repeated uint64 bytes = 1 [packed = true];
      repeated uint64 blocks = 2 [packed = true];
    }

    required string type = 1;
    required string name = 2;
    optional uint64 length = 3;
    optional Data data = 4;
  }
`)
