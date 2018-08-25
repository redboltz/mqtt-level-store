'use strict'

/* eslint-env mocha */

var abstractTest = require('mqtt/test/abstract_store')
var level = require('level-test')()
var mqttLevelStore = require('./')
var mqtt = require('mqtt')
var Connection = require('mqtt-connection')
var concat = require('concat-stream')
var net = require('net')

describe('mqtt level store', function () {
  abstractTest(function (done) {
    done(null, mqttLevelStore.single({ level: level() }))
  })
})

describe('mqtt level store manager', function () {
  var manager

  beforeEach(function () {
    manager = mqttLevelStore({ level: level() })
  })

  afterEach(function (done) {
    manager.close(done)
  })

  describe('incoming', function () {
    abstractTest(function (done) {
      done(null, manager.incoming)
    })
  })

  describe('outgoing', function () {
    abstractTest(function (done) {
      done(null, manager.outgoing)
    })
  })
})

describe('mqtt.connect flow', function () {
  var server
  var manager

  beforeEach(function (done) {
    server = new net.Server()
    server.listen(8883, done)

    server.on('connection', function (stream) {
      var client = Connection(stream)

      client.on('connect', function () {
        client.connack({returnCode: 0})
      })

      server.emit('client', client)
    })

    manager = mqttLevelStore({ level: level() })
  })

  it('should resend messages', function (done) {
    var client = mqtt.connect({
      port: 8883,
      incomingStore: manager.incoming,
      outgoingStore: manager.outgoing
    })
    var count = 0
    client.publish('hello1', 'world1', {qos: 1})
    client.publish('hello2', 'world2', {qos: 1})
    client.publish('hello3', 'world3', {qos: 1})

    server.once('client', function (serverClient) {
      serverClient.once('publish', function () {
        serverClient.stream.destroy()

        manager.outgoing.createStream().pipe(concat(function (list) {
          list.length.should.equal(3)
        }))
      })
      server.once('client', function (serverClient2) {
        serverClient2.on('publish', function (packet) {
          console.log(packet)
          serverClient2.puback(packet)
          if (++count === 3) {
            client.end()
            done()
          }
        })
      })
    })
  })
})
