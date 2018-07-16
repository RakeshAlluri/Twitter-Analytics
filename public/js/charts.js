var socket = io.connect('http://104.154.162.253:3000')
var series =  [{name: 'Count',data: []}];

var title = {
   text: '"Denver" Twitter Mentions'   
};
var subtitle = {
   text: 'Source: Twitter'
};
var xAxis = {
   categories: []
};
var yAxis = {
   title: {
      text: 'Mentions'
   },
   plotLines: [{
      value: 0,
      width: 1,
      color: '#808080'
   }]
};   


var legend = {
   layout: 'vertical',
   align: 'right',
   verticalAlign: 'middle',
   borderWidth: 0
};


var json = {};
json.title = title;
json.subtitle = subtitle;
json.xAxis = xAxis;
json.yAxis = yAxis;
json.legend = legend;
json.series = series;

var chart = Highcharts.chart('container',json)

/****************** Socket Connection to the server *************************/

socket.on('connect', function(data) {
   console.log('socket connection made with the server')
});


/*************** Socket on receiving a message from chat *******************/

socket.on('chat',function(data){
   console.log(data)
   console.log(typeof(data))
   a = JSON.parse(data)
   console.log(a.count)
   var endDate = new Date(a.end)
   var endMinutes = (endDate.getHours()*60)+endDate.getMinutes()
   if (chart.series[0].data.length >= 20) {
      chart.series[0].data[0].remove();
   }
   chart.series[0].addPoint([endMinutes,parseInt(a.count)])

 })

window.onload=reviewHighlights();

function reviewHighlights(callback){
var xhr = new XMLHttpRequest(),
    method = "GET",
    url = "/reviewHighlights";
xhr.open(method, url, true);
xhr.onreadystatechange = function () {
        if(xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
            var res = xhr.responseText;
            res = JSON.parse(res)
            console.log(res[0].text)
            for(var i=0; i < 10; i++){
               document.getElementById("highlightsContainer").innerHTML +='';
               document.getElementById("highlightsContainer").innerHTML +=
               '<div id="highlights"><h3 id="phrases">'+res[i].phrases+
               '</h3><h3 id="count">in '+res[i].count+' tweets</h3><h3 id="text">'+res[i].text+'</h3><hr width="400" </div>';
         }
            console.log(res)
        }
    };
xhr.send(null);

}


            


