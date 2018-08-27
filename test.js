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
    console.log('new server')
    server = new net.Server()
    console.log('server listen')
    server.listen(8883, done)

    console.log('server before connect')
    server.on('connection', function (stream) {
      console.log('server after connect')
      var client = Connection(stream)

      client.on('connect', function () {
        console.log('client on connect')
        client.connack({returnCode: 0})
      })

      console.log('emit client')
      server.emit('client', client)
    })

    console.log('create level store')
    manager = mqttLevelStore({ level: level() })
  })

  afterEach(function (done) {
    this.timeout(10000)
    console.log('after each')
    manager.close(function () {
      console.log('manager closed')
      server.close(function () {
        console.log('server closed')
        done()
      })
    })
  })

  it('should resend messages', function (done) {
    this.timeout(10000)
    var client = mqtt.connect({
      port: 8883,
      incomingStore: manager.incoming,
      outgoingStore: manager.outgoing
    })

    client.publish('hello', 'world', {qos: 1})

    server.once('client', function (serverClient) {
      serverClient.once('publish', function () {
        serverClient.stream.destroy()

        manager.outgoing.createStream().pipe(concat(function (list) {
          list.length.should.equal(1)
        }))
      })

      server.once('client', function (serverClient2) {
        serverClient2.once('publish', function (packet) {
          serverClient2.puback(packet)
          client.end()
          done()
        })
      })
    })
  })

  it('should resend messages by published order', function (done) {
    this.timeout(10000)
    var serverCount = 0
    var client = mqtt.connect({
      port: 8883,
      incomingStore: manager.incoming,
      outgoingStore: manager.outgoing
    })

    client.nextId = 65535
    client.publish('topic', 'payload1', {qos: 1})
    client.publish('topic', 'payload2', {qos: 1})
    client.publish('topic', 'payload3', {qos: 1})
    server.once('client', function (serverClient) {
      console.log('1st connect')
      serverClient.on('publish', function () {
        console.log('disconnect')

        serverClient.stream.destroy()

        manager.outgoing.createStream().pipe(concat(function (list) {
          console.log('length check')
          list.length.should.equal(3)
        }))
        server.once('client', function (serverClient2) {
          console.log('2nd connect')
          serverClient2.on('publish', function (packet) {
            console.log(packet)
            serverClient2.puback(packet)
            switch (serverCount++) {
              case 0:
                packet.payload.toString().should.equal('payload1')
                break
              case 1:
                packet.payload.toString().should.equal('payload2')
                break
              case 2:
                packet.payload.toString().should.equal('payload3')
                setTimeout(function () {
                  // make sure additional publish shouldn't be received
                  serverCount.should.equal(3)
                  client.end()
                  done()
                }, 200)
                break
              default:
                break
            }
          })
        })
      })
    })
  })
})
