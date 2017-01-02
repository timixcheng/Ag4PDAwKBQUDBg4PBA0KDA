var promise = require("bluebird");
var beanstalk_client = require('beanstalk_client');
promise.promisifyAll(beanstalk_client.Client);
var config = require('config');
var rp = require('request-promise');
var mongodb = require('mongodb');
var mongoClient = promise.promisifyAll(mongodb.MongoClient);

function ExchangeJob(){
    var self = this;
    this.error_count = 0;
    this.error_tolerance = config.get('ErrorTolerance');
    
    // define private function
    this.loop = function(func, index, stop) {
        if (index < stop) {
            var delay_sec = index > 0 ? config.get('DelaySecPerJob') * 1000 : 0;
            return promise.delay(delay_sec).then(() => {
                console.log('index=',index);
                func();
                return this.loop(func, index+1, stop);
            });
        }
    }

    this.retry = function(func){
        return func().catch(function (err) {
            console.log("error occurs!" + err);
            console.log('error_count=' + this.error_count);
            if (this.error_count > this.error_tolerance) {
                throw err;
            }
            // delay 3s
            console.log('delaying 3s');
            return promise.delay(config.get('DelayPerError') * 1000).then(function(){
                console.log('awake');
                error_tolerance--;
                return self.retry(func); 
            });
        });
    };

    this.getConnection = function(){
        return mongoClient.connectAsync(config.get('ConnectionString')).disposer(function(db) {
            console.log('db close');
            db.close();
        });
    };

    this.writeRate = function(from, to, rate){
        var timestamp = new Date();
        var dataEntry = {
            from: from,
            to: to,
            created_at: timestamp,
            rate: rate
        };
        // write to mongo
        return promise.using(self.getConnection(), (db) => {
            console.log('DB connected');
            var collection = db.collection('rate_data');
            return collection.insertOne(dataEntry).then(function(){
                console.log('Inserted');
            });
        });
    };

    this.getExchange = function(from, to){
        // Options to be used by request 
        var options = {
            uri: 'http://api.fixer.io/latest?base='+from+'&symbols='+to,
            json: true
        };
        return rp(options).then(function(body){
            console.log('rate got');
            var rate = body.rates[to];
            return self.writeRate(from, to, Math.round(rate * 100) / 100);   
        });
    };

    this.handleJob = function(job_id, job_payload){
        var job_data = JSON.parse(job_payload);
        console.log('from=' + job_data.from + ',to=' + job_data.to);
        return self.getExchange(job_data.from, job_data.to);
    };

    this.fetchAndHandleJob = function(conn){
        console.log('fetchAndHandleJob starts');
        var id;
        conn.watchAsync(config.get('Beanstalk.tube')).then(function(){
        return conn.reserveAsync();
        }).spread(function(job_id, job_payload){
            id = job_id;
            console.log('Job ' + id + ' fetched');
            // insert retry here, in order to bury the job if max error tolerant is hit
            return self.retry(function() { return self.handleJob(job_id, job_payload); });
        }).catch(function (err){
            console.log('Max error tolerant hits. Bury the job '+id);
            conn.destroyAsync(id);
            throw err;
        }).then(function() {
            return conn.destroyAsync(id);
        }).then(function() {
            console.log('Job '+id+' completed');
        });
    };

    this.singleConventionJob = function (){
        console.log('singleConventionJob starts');
        var client = beanstalk_client.Client;
        return client.connectAsync(config.get('Beanstalk.host')).then(function(conn) {
            console.log('beanstalk connected');
            promise.promisifyAll(conn, {multiArgs: true});
            return conn;
        }).then((conn) => {
            return self.fetchAndHandleJob(conn);
        });
    };
}

ExchangeJob.prototype.run = function () {
    console.log('run!');
    this.loop(() =>{
        return this.retry(this.singleConventionJob);
    },0, config.get('TotalJobPerRun'));
};

module.exports = ExchangeJob;
