var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require("async");
var upsertSong = 'INSERT INTO simplex.songs (id, title, album, artist, tags, data)  '
    + 'VALUES(?, ?, ?, ?, ?, ?);';
var getSongById = 'SELECT * FROM simplex.songs WHERE id = ?;';
var getFlightById = 'SELECT * FROM demo.flights_load WHERE id = ?;';

var client = new cassandra.Client( { contactPoints : [ '127.0.0.1' ] } );
client.connect(function(err, result) {
    console.log('Connected.');
});

var app = express();
app.use(bodyParser.json());
app.set('json spaces', 2);

app.get('/metadata', function(req, res) {
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});

app.post('/keyspace', function(req, res) {
    client.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " + 
                   "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                   afterExecution('Error: ', 'Keyspace created.', res));
});

app.post('/tables', function(req, res) {
    async.parallel([
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS simplex.songs (' +
                'id uuid PRIMARY KEY,' +
                'title text,' +
                'album text,' +
                'artist text,' +
                'tags set<text>,' +
                'data blob' +
                ');',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS simplex.playlists (' +
                'id uuid,' +
                'title text,' +
                'album text,' +
                'artist text,' +
                'song_id uuid,' +
                'PRIMARY KEY (id, title, album, artist)' +
                ');',
                next);
        }
    ], afterExecution('Error: ', 'Tables created.' , res));
});

app.post('/song', function(req, res) {
    var id = null;
    if ( ! req.body.hasOwnProperty('id')) {
        id = cassandra.types.uuid();
    } else {
        id = req.body.id;
    }
    client.execute(upsertSong,
        [id, req.body.title, req.body.album, req.body.artist, req.body.tags, null],
        afterExecution('Error: ', 'Song ' + req.body.title + ' upserted.', res));
});

app.get('/song/:id', function(req, res) {
    client.execute(getSongById, [ req.params.id ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Song not found.' });
        } else {
            res.json(result);        }
    });
});

app.get('/FlightById/:id', function(req, res) {
    client.execute(getFlightById, [ req.params.id ], { hints : ['int'] }, function(err, result) {
        if (err) {
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

