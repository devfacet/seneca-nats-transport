/*
 * seneca-nats-transport
 * Copyright (c) 2015 Fatih Cetinkaya (http://github.com/cmfatih/seneca-nats-transport)
 * For the full copyright and license information, please view the LICENSE.txt file.
 */

/* jslint node: true */
'use strict';

var nats = require('nats'),
    _    = require('lodash');

module.exports = function(options) {

  // Defaults
  var seneca = this,
      plugin = 'nats-transport';

  var senecaOpts  = seneca.options(),
      transpUtils = seneca.export('transport/utils');

  options = seneca.util.deepextend({
    nats: {
      url:                  'nats://localhost:4222',
      reconnect:            true,
      maxReconnectAttempts: 9999,
      reconnectTimeWait:    1000
    }
  }, senecaOpts.transport, options);

  // Listen hook for the transport
  seneca.add({role: 'transport', type: 'nats', hook: 'listen'}, function(msg, done) {

    var type       = msg.type,
        clientOpts = seneca.util.clean(_.extend({}, options[type], msg)),
        clientName = 'listen-' + type,
        nc         = nats.connect(options[type]);

    // Connect event
    nc.on('connect', function(/*client*/) {
      seneca.log.info('listen', 'open', clientOpts);
    });

    // Error event
    nc.on('error', function(err) {
      seneca.log.error('listen', 'error', err);
    });

    // Listen topics
    transpUtils.listen_topics(seneca, msg, clientOpts, function(topic) {
      var topicAct = topic + '_act',
          topicRes = topic + '_res';

      // Subscribe to act topic
      nc.subscribe(topicAct, function(msg) {
        seneca.log.debug('listen', 'subscribe', topicAct, 'message', msg);

        // Handle request
        transpUtils.handle_request(seneca, transpUtils.parseJSON(seneca, clientName, msg), clientOpts, function(out) {
          // If there is an output then
          if(out) {
            // Publish it to response topic
            nc.publish(topicRes, transpUtils.stringifyJSON(seneca, clientName, out));
          }
        });
      });
      seneca.log.info('listen', 'subscribe', topicAct);
    });

    // Closer action
    seneca.add({role: 'seneca', cmd: 'close'}, function(args, cb) {
      seneca.log.debug('listen', 'close', clientOpts);

      nc.close();
      this.prior(args, cb);
    });

    done();
  });

  // Client hook for the transport
  seneca.add({role: 'transport', type: 'nats', hook: 'client'}, function(msg, done) {

    var type       = msg.type,
        clientOpts = seneca.util.clean(_.extend({}, options[type], msg)),
        clientName = 'client-' + type,
        nc         = nats.connect(options[type]);

    // Connect event
    nc.on('connect', function(/*client*/) {
      seneca.log.info('client', 'open', clientOpts);
    });

    // Error event
    nc.on('error', function(err) {
      seneca.log.error('client', 'error', err);
    });

    // Send is called for per topic
    function send(spec, topic, sendDone) {
      var topicAct = topic + '_act',
          topicRes = topic + '_res';

      // Subscribe to response topic
      nc.subscribe(topicRes, function(msg) {
        seneca.log.debug('client', 'subscribe', topicRes, 'message', msg);

        // Handle response
        transpUtils.handle_response(seneca, transpUtils.parseJSON(seneca, clientName, msg), clientOpts);
      });
      seneca.log.info('client', 'subscribe', topicRes);

      // Send message over the transport
      sendDone(null, function(msg, cb) {
        seneca.log.debug('client', 'publish', topicAct, 'message', msg);

        // Publish act
        nc.publish(topicAct, transpUtils.stringifyJSON(seneca, clientName, transpUtils.prepare_request(seneca, msg, cb)));
      });

      // Closer action
      seneca.add({role: 'seneca', cmd: 'close'}, function(args, cb) {
        seneca.log.debug('client', 'close', clientOpts, 'topic', topic);

        nc.close();
        this.prior(args, cb);
      });
    }

    // Use transport utils to make client
    transpUtils.make_client(send, clientOpts, done);
  });

  // Return
  return {
    name: plugin
  };

};