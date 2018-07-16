var express = require('express');
var Twitter = require('twitter');
var app = express();
var fs = require('fs');
var path = require('path');
var scriptName = path.basename(__filename);
var myWriteStream = fs.createWriteStream(__dirname+'twitter.txt');



/***************** Reading properties file ******************/

var contents = fs.readFileSync('authKeys.json');
var jsonContent = JSON.parse(contents);


/***************** Twitter Authentication ******************/

var twitterClient = new Twitter({
                          consumer_key: jsonContent.twitter.consumer_key,
                          consumer_secret: jsonContent.twitter.consumer_secret,
                          access_token_key: jsonContent.twitter.access_token_key,
                          access_token_secret: jsonContent.twitter.access_token_secret
                          });
                         
twitterClient.stream('statuses/filter', {track:'target'},  function(stream) {
	//{track:'trader joes,traderjoes,trader joe\'s,traderjoe\'s'}
	stream.on('data', function(tweet) {
		//myWriteStream.write(JSON.stringify(tweet));
	    sendToKafka(JSON.stringify(tweet));
	});

  	stream.on('error', function(error) {
    	console.log(error);
  	});
});         


/*************** Kafka connection **************************/
var kafka = require('kafka-node'),
                HighLevelConsumer = kafka.HighLevelConsumer,
                HighLevelProducer = kafka.HighLevelProducer,
                kClient = new kafka.Client('localhost:2181'),
				producer = new HighLevelProducer(kClient),
                consumer = new HighLevelConsumer(kClient,[{ topic: 'twitterstream' }],{groupId: 'my-group'});


/***Kafka producer on ready state**/
producer.on('ready', function () {});

/***Kafka producer on error state**/
producer.on('error', function(err){
	console.log(err)
})

var sendToKafka = function(tweet){
	//console.log('sending tweet to kafka: '+tweet)
	payloads = [{ topic: 'twitterstream',messages: tweet, partition: 0 }];
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
}
