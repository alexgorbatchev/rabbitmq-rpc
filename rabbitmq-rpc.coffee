amqp = require 'amqp'
uuid = require 'uuid'

module.exports = class RabbitMQRPC
  connect: (opts, done) ->
    @subscriptionsPending = 0
    @connection = amqp.createConnection opts
    @connection.once 'ready', =>
      @exchange = @connection.exchange 'rpc-exchange123a', confirm: yes, type: 'topic'
      @exchange.on 'error', (err) -> console.log err

      done()

  subscribe: (routingKey, callback) ->
    @subscriptionsPending++

    @connection.queue "rpc-queue-#{routingKey}", durable: yes, (queue) =>
      queue.on 'queueBindOk', =>
        promise = queue.subscribe ack: yes, prefetchCount: 1, (message, headers, deliveryInfo) =>
          callback null, message, (err, response) =>
            {correlationId} = deliveryInfo
            @exchange.publish correlationId, response or null, {correlationId, mandatory: yes}
            queue.shift()

        promise.addCallback => @subscriptionsPending--

      queue.on 'error', (err) -> console.log err

      queue.bind(@exchange, routingKey)

  publish: (routingKey, message, callback) ->
    if @subscriptionsPending isnt 0
      return setTimeout =>
        @publish routingKey, message, callback
      , 1

    correlationId = "#{routingKey}-#{uuid.v4()}"
    send = => @exchange.publish routingKey, message, {correlationId, mandatory: yes}

    @connection.queue routingKey, durable: yes, (queue) =>
      queue.on 'queueBindOk', =>
        promise = queue.subscribe ack: yes, prefetchCount: 1, (message, headers, deliveryInfo) =>
          # if deliveryInfo.correlationId is correlationId
          queue.unsubscribe deliveryInfo.consumerTag
          callback? null, message
          queue.shift()

        promise.addCallback send
        promise.addErrback (err) -> console.log err

      queue.on 'error', (err) -> console.log err

      queue.bind(@exchange, correlationId)
