var hypercore = require('hypercore')
var from = require('from2')
var collect = require('stream-collector')
var thunky = require('thunky')
var messages = require('./messages')

module.exports = Drive

function Drive (db) {
  if (!(this instanceof Drive)) return new Drive(db)
  this.core = hypercore(db)
}

Drive.prototype.replicate = function () {
  return this.core.replicate()
}

Drive.prototype.createArchive = function (key, opts) {
  if (typeof key === 'object' && !Buffer.isBuffer(key) && key) {
    opts = key
    key = null
  }
  return new Archive(this, key, opts)
}

function Archive (drive, key, opts) {
  if (!opts) opts = {}

  this.options = opts || {}
  this.storage = this.options.storage
  delete this.options.storage

  this.drive = drive
  this.live = this.options.live !== false
  this.metadata = drive.core.createFeed(key, this.options)
  this.contents = null
  this.key = key || this.metadata.key
  this.owner = !key
  this.open = thunky(open)

  this._offset = 1
  this._bytes = 0

  var self = this

  function open (cb) {
    self._open(cb)
  }
}

Archive.prototype.replicate = function () {
  var self = this
  var stream = this.metadata.replicate()

  this.open(function (err) {
    if (err) return stream.destroy(err)
    self.contents.join(stream)
  })

  return stream
}

Archive.prototype.join = function (stream) {
  this.open(function (err) {
    if (err) return stream.destroy(err)
    self.metadata.join(stream)
    self.contents.join(stream)
  })
}

Archive.prototype.leave = function (stream) {
  this.open(function (err) {
    if (err) return stream.destroy(err)
    self.metadata.leave(stream)
    self.contents.leave(stream)
  })
}

Archive.prototype.list = function (cb) {
  var self = this
  var offset = 0
  var live = !cb

  return collect(from.obj(read), cb)

  function read (size, cb) {
    if (!offset) return open(cb)
    if (offset === self.metadata.blocks && !live) return cb(null, null)
    self.stat(offset++ - self._offset, cb)
  }

  function open (cb) {
    self.open(function (err) {
      if (err) return cb(err)
      offset = self._offset
      if (!self.live) live = false
      read(16, cb)
    })
  }
}

Archive.prototype.stat = function (index, cb) {
  this.metadata.get(index + this._offset, function (err, data) {
    if (err) return cb(err)
    cb(null, messages.Entry.decode(data))
  })
}

Archive.prototype.createFileReadStream = function (index) {
  var self = this
  var stat = null
  var start = 0
  var end = 0

  return from(read)

  function read (size, cb) {
    if (!stat) return open(cb)
    if (end <= start) return cb(null, null)
    self.contents.get(start++, cb)
  }

  function open (cb) {
    self.open(function (err) {
      if (err) return cb(err)
      if (typeof index === 'object') {
        stat = index
        return read(16, cb)
      }

      self.stat(index, function (err, result) {
        if (err) return cb(err)
        stat = result
        if (stat.data) {
          start = stat.data.blocks[0] || 0
          end = stat.data.blocks[1] || 0
        }
        read(16, cb)
      })
    })
  }
}

Archive.prototype.add = function (name, cb) {
  if (!this.storage) throw new Error('Set options.storage to use this method')
  if (!cb) cb = noop

  var self = this
  var store = this.storage(name, this.options)
  var offset = 0
  var startBlock = 0
  var startBytes = 0

  this.open(open)

  function open (err) {
    if (err) return cb(err)
    startBlock = self.contents.blocks
    startBytes = self._bytes
    self.options.storage.readonly = true
    if (store.open) store.open(loop)
    else loop(null)
  }

  function loop (err) {
    if (err) return cb(err)
    if (offset >= store.length) return finalize()
    var chunk = Math.min(store.length - offset, 16 * 1024)
    store.read(offset, chunk, afterRead)
  }

  function afterRead (err, data) {
    if (err) return cb(err)
    offset += data.length
    self.contents.append(data, loop)
  }

  function finalize () {
    self.options.storage.readonly = false
    self._bytes += offset
    var entry = {
      type: 'file',
      name: name,
      length: offset,
      data: !offset ? null : {
        blocks: [startBlock, self.contents.blocks],
        bytes: [startBytes, self._bytes]
      }
    }

    if (store.close) store.close()
    self.metadata.append(messages.Entry.encode(entry), cb)
  }
}

Archive.prototype._open = function (cb) {
  var self = this
  this.metadata.open(function (err) {
    if (err) return cb(err)

    if (!self.owner && self.metadata.secretKey) self.owner = true // TODO: hypercore should tell you this

    if (!self.owner || self.metadata.blocks) {
      self.metadata.get(0, function (err, header) {
        if (err) return cb(err)
        header = JSON.parse(header)
        self.live = header.live
        if (header.version && header.version !== 0) return cb(new Error('Unsupported archive format. Upgrade hyperdrive.'))

        if (self.live) self._offset = 2
        else self._offset = 1

        self.metadata.get(self.live ? 1 : self.metadata.blocks - 1, function (err, content) {
          if (err) return cb(err)
          content = messages.Content.decode(content)
          self.options.key = content.contentFeed
          if (self.storage) self.options.storage = new Storage(self)
          self.contents = self.drive.core.createFeed(self.options)
          self.contents.open(function (err) {
            if (err) return cb(err)

            if (self.contents._merkle) {
              for (var i = 0; i < self.contents._merkle.roots.length; i++) {
                self._bytes += self.contents._merkle.roots[i].size
              }
            }

            cb()
          })
        })
      })
      return
    }

    var header = {
      type: 'hyperdrive',
      live: self.options.live
    }

    if (self.storage) self.options.storage = new Storage(self)
    self.contents = self.drive.core.createFeed(null, self.options)

    var batch = [Buffer(JSON.stringify(header))]
    if (header.live) batch.push(messages.Content.encode({contentFeed: self.contents.key}))

    self._offset = batch.length
    self.metadata.append(batch, function (err) {
      if (err) return cb(err)
      self.contents.open(cb)
    })
  })
}

Archive.prototype.finalize = function (cb) {
  if (!cb) cb = noop
  var self = this
  this.contents.finalize(function (err) {
    if (err) return cb(err)
    if (!self.live) self.metadata.append(messages.Content.encode({contentFeed: self.contents.key}))
    self.metadata.finalize(function (err) {
      if (err) return cb(err)
      self.key = self.metadata.key
      cb()
    })
  })
}

function Storage (archive) {
  this.archive = archive
  this.closed = false
  this.readonly = false
  this._stats = []
}

Storage.prototype.read = function (offset, length, cb) {
  if (this.closed) return cb(new Error('Storage is closed'))
  var st = this._find(offset)
  if (!st) return this._load(offset, length, null, cb)
  st.storage.read(offset - st.start, length, cb)
}

Storage.prototype.write = function (offset, data, cb) {
  if (this.readonly) return cb(null)
  if (this.closed) return cb(new Error('Storage is closed'))
  var st = this._find(offset)
  if (!st) return this._load(offset, data.length, data, cb)
  st.storage.write(offset - st.start, data, cb)
}

Storage.prototype.close = function (cb) {
  var self = this
  this.closed = true
  loop(null)

  function loop (err) {
    if (!self._stats.length) return cb()
    var next = self._stats.shift()
    if (next.close) next.close(loop)
    else process.nextTick(loop)
  }
}

Storage.prototype._find = function (offset) { // TODO: should do some binary search instead
  for (var i = 0; i < this._stats.length; i++) {
    var stat = this._stats[i]
    if (stat.start <= offset && offset < stat.end) return stat
  }
  return null
}

Storage.prototype._load = function (offset, length, data, cb) {
  var self = this
  var i = 0

  loop(null, null)

  function loop (err, stat) {
    if (err) return cb(err)
    if (self.closed) return cb(new Error('Storage is closed'))

    if (stat && stat.data && stat.data.bytes[0] <= offset && stat.data.bytes[1] > offset) {
      var st = {
        start: stat.data.bytes[0],
        end: stat.data.bytes[1],
        storage: self.archive.storage(stat.name, self.options)
      }

      self._stats.push(st)
      if (st.storage.open) st.storage.open(done) // TODO: lru cache to close stores after 128 opens etc
      else done(null)
    }

    self.archive.stat(i++, loop)
  }

  function done (err) {
    if (err) return cb(err)
    if (data) self.write(offset, data, cb)
    else self.read(offset, length, cb)
  }
}

function noop () {}

var d = Drive(require('memdb')())
var d1 = Drive(require('memdb')())

var archive = d.createArchive({
  storage: function (name) {
    return require('random-access-file')(name)
  }
})

var archive1 = d1.createArchive(archive.key, {
  storage: function (name) {
    return require('random-access-file')('sandbox/' + name, {truncate: true})
  }
})

// archive1.list().on('data', console.log).on('end', console.log.bind(console, 'end'))

// if (require.main !== module) return

archive.add('index.js', function () {
  // archive.add('sintel.mp4')
})

var stream = archive.replicate()
stream.pipe(archive1.replicate()).pipe(stream)
