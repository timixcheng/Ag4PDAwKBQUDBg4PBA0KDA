var http = require("http");
var ExchangeJob = require('./ExchangeJob.js');

http.createServer(function (request, response) {
    console.log('connected!');
    var job = new ExchangeJob();
    job.run();
    response.writeHead(200, {'Content-Type': 'text/plain'});  
    response.end('Start!');
}).listen(3000);
console.log('Server running');
