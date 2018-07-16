var express = require('express');
var Twitter = require('twitter');
var app = express();
var socket = require('socket.io');
var server = app.listen(3000,function(){
	console.log('Listening to port 3000')
});

var mysql = require('mysql');

var con = mysql.createConnection({
  host: "10.128.0.2",
  user: "hive",
  password: "admin",
  database:"twitter"
});

con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});


con.query('SELECT * FROM review_highlights', function(err,rows) {
  if(err) throw err;
  console.log('Data received from Db:\n');
  //console.log(rows);
});

var kafka = require('kafka-node'),
	HighLevelConsumer = kafka.HighLevelConsumer,
 	client = new kafka.Client('localhost:2181'),
 	Producer = kafka.Producer,
 	producer = new Producer(client),
	consumer = new HighLevelConsumer(
    	client,
    	[
        	{ topic: 'test1' }
    	],
    	{
        	groupId: 'my-group'
    	}
	);


/****************** Initializing static routes ***************************/
app.use(express.static(__dirname+'/public/html'));
app.use(express.static(__dirname+'/public/css'));
app.use(express.static(__dirname+'/public/js'));


/****************** Serving HTML file ***********************************/
app.get('/',function(req,res){
	res.sendFile('index.html')
})

app.get('/charts.html',function(req,res){
	res.sendFile('charts.html')
})

app.get('/reviewHighlights', function(req, res, next) {
  con.query('SELECT * FROM review_highlights', function(err,rows) {
  console.log('Data received from Db:\n');
  if(err) throw err;
  res.json(rows);
  });
});


/*******************  Socket Connection ********************************/

var io = socket(server);
var data = {message: [80,90]}
console.log(data)
io.on('connection', function(socket){
	console.log('made socket connection',socket.id);
	consumer.on('message', function (message) {
		console.log(message)
		socket.emit('chat',message.value);
	})

});


