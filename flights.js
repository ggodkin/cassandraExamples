var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require("async");

var getFlightById = 'SELECT * FROM demo.flights_load WHERE id = ?;';
var getFlightSched = 'select * from demo.flights_qa where origin = ? and year = ? and fl_date = ? LIMIT ?;';
var getFlightSchedL = 'select * from demo.flights_qa where origin = ? and year = ?  LIMIT ?;';
//var getFlightSched = 'SELECT * FROM demo.flights_qa WHERE origin = ? AND year = ? LIMIT 10;';

var client = new cassandra.Client( { contactPoints : [ '127.0.0.1' ] } );
client.connect(function(err, result) {
    console.log('Connected.');
});

var app = express();
app.use(bodyParser.json());
app.set('json spaces', 2);

/*app.get('/metadata', function(req, res) {
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});*/


app.get('/FlightById/:id', function(req, res) {
    client.execute(getFlightById, [ req.params.id ], { hints : ['int'] }, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Flight not found. '});
        } else {
            res.json(result);        }
    });
});

app.get('/FlightSched/:origin,:year,:fl_date,:rlimit', function(req, res) {
    client.execute(getFlightSched, [ req.params.origin, req.params.year, req.params.fl_date, req.params.rlimit ], { hints : ['text','int','timestamp','int'] }, function(err, result) {
        if (err) {
            console.log('Passing origi %s, Year %s, Flight Date %s. Returned error: %s', req.params.origin, req.params.year, req.params.fl_date, err);
            res.status(404).send({ msg : 'Flight not found. '});
        } else {
            res.json(result);        }
    });
});

app.get('/FlightSchedL/:origin,:year,:rlimit', function(req, res) {
    client.execute(getFlightSchedL, [ req.params.origin, req.params.year, req.params.rlimit ], { hints : ['text','int','int'] }, function(err, result) {
        if (err) {
            console.log('Passing origi %s, Year %s. Returned error: %s', req.params.origin, req.params.year, err);
            res.status(404).send({ msg : 'Flight not found. '});
        } else {
            res.json(result);        }
    });
});

function afterExecution(errorMsessage, successMessage) {
    return function(err, result) {
        if (err) {
            return console.log(errorMessage);
        } else {
            return console.log(successMessage);
        }
    }
}

var server = app.listen(3000, function() {
    console.log('Listening on port %d', server.address().port);
});

