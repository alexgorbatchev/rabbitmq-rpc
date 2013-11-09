chai = require 'chai'
sinon = require 'sinon'

# explicitly use compiled version
RabbitMQRPC  = require '../rabbitmq-rpc.js'

expect = chai.expect
chai.use require 'sinon-chai'

describe 'rabbitmq-rpc', ->
  rpc = null

  it 'connects', (done) ->
    rpc = new RabbitMQRPC()
    rpc.connect url: 'amqp://guest:guest@localhost:5672', ->
      expect(rpc).to.be.ok
      expect(rpc.connection).to.be.ok
      done()

  describe 'with callback', ->
    sentMessage = results = null
    spy1 = spy2 = null

    before ->
      spy1 = sinon.spy()
      spy2 = sinon.spy()

    it 'subscribes', ->
      rpc.subscribe 'm1', (err, message, callback) ->
        process.nextTick ->
          spy1 message
          callback null, response: message

    it 'publishes and receives response', (done) ->
      sentMessage = {foo: 'bar'}
      rpc.publish 'm1', sentMessage, (err, r) ->
        results = r
        done()

    it 'calls subscriber once', -> expect(spy1).to.have.been.calledOnce
    it 'passes right data to subscriber', -> expect(spy1).to.have.been.calledWith sentMessage
    it 'receives correct results', -> expect(results).to.eql response: sentMessage

  describe 'without callback', ->
    sentMessage = null
    spy1 = spy2 = null

    before ->
      spy1 = sinon.spy()
      spy2 = sinon.spy()

    it 'subscribes', ->
      rpc.subscribe 'm1', (err, message, callback) ->
        process.nextTick ->
          spy1 message
          done()

    it 'publishes and receives response', (done) ->
      rpc.subscribe 'm2', (err, message, callback) ->
        process.nextTick ->
          spy2 message
          callback()
          done()

      sentMessage = {no: 'callback'}
      rpc.publish 'm2', sentMessage

    it 'calls subscriber once', -> expect(spy2).to.have.been.calledOnce
    it 'passes right data to subscriber', -> expect(spy2).to.have.been.calledWith sentMessage
    it 'does not call other subscriber', -> expect(spy1).to.have.not.been.called

  describe 'multiple subscribers', ->
    results1 = []
    results2 = []
    spy1 = spy2 = null

    before ->
      spy1 = sinon.spy()
      spy2 = sinon.spy()

    it 'subscribes', ->
      rpc.subscribe 'm3', (err, message, callback) ->
        process.nextTick ->
          spy1 message
          callback null, msg: 'subscriber1'

      rpc.subscribe 'm3', (err, message, callback) ->
        process.nextTick ->
          spy2 message
          callback null, msg: 'subscriber2'

      # setTimeout done, 100

    it 'publishes first', (done) ->
      rpc.publish 'm3', {}, (err, r) ->
        results1.push r
        done()

    it 'publishes second', (done) ->
      rpc.publish 'm3', {}, (err, r) ->
        results2.push r
        done()

    it 'publishes first again', (done) ->
      rpc.publish 'm3', {}, (err, r) ->
        results1.push r
        done()

    it 'publishes second again', (done) ->
      rpc.publish 'm3', {}, (err, r) ->
        results2.push r
        done()

    it 'calls first subscriber twice', -> expect(spy1).to.have.been.calledTwice
    it 'calls second subscriber twice', -> expect(spy2).to.have.been.calledTwice
    it 'returns correct accumulated results from first subscriber', -> expect(results1).to.eql [{msg: 'subscriber1'}, {msg: 'subscriber1'}]
    it 'returns correct accumulated results from second subscriber', -> expect(results2).to.eql [{msg: 'subscriber2'}, {msg: 'subscriber2'}]

  describe 'long running jobs', ->
    spy1 = spy2 = spy3 = time = null

    before ->
      spy1 = sinon.spy()
      spy2 = sinon.spy()
      spy3 = sinon.spy()

    it 'publishes 10 times to two slow workers', (done) ->
      counter = 0

      rpc.subscribe 'm4', (err, message, callback) ->
        setTimeout ->
          spy1 message
          callback null, message
        , 100

      rpc.subscribe 'm4', (err, message, callback) ->
        setTimeout ->
          spy2 message
          callback null, message
        , 100

      for index in [1..10]
        do (index) ->
          rpc.publish 'm4', {index}, (err, results) ->
            spy3 results
            expect(results.index).to.eql index

            if ++counter is 10
              done()

    it 'published 10 times', -> expect(spy3.callCount).to.eql 10
    it 'processed 5 times in one worker', -> expect(spy1.callCount).to.eql 5
    it 'processed 5 times in another worker', -> expect(spy2.callCount).to.eql 5
