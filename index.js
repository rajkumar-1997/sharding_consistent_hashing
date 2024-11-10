const express = require('express');
const cors = require('cors');
const { Client } = require('pg');
const hashring = require('hashring');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const clients = {
  shard1: new Client({
    host: 'pgshard1',
    port: 5432,
    password: 'postgres',
    database: 'postgres',
    user: 'postgres',
  }),
  shard2: new Client({
    host: 'pgshard2',
    port: 5432,
    password: 'postgres',
    database: 'postgres',
    user: 'postgres',
  }),
  shard3: new Client({
    host: 'pgshard3',
    port: 5432,
    password: 'postgres',
    database: 'postgres',
    user: 'postgres',
  }),
};

const hr = new hashring();
hr.add('shard1');
hr.add('shard2');
hr.add('shard3');

connect();

app.get('/getUrl/:url_id', (req, res) => {
  try {
    const hash = req.params.url_id;

    const client = clients[hr.get(hash)];

    const result = client('SELECT * from url_table where url_id=$1', [
      id,
      url,
      url_id,
    ]);

    if (result.rows.length > 0) {
      res.status(200).send({ data: result.rows[0] });
    } else {
      res.status(404).send({ error: 'URL not found' });
    }
  } catch (error) {
    console.error('Error in getUrl:', error);
    res.status(500).send({ error: 'Failed to fetch URL' });
  }
});

app.post('/addUrl', (req, res) => {
  try {
    const url = req.body.url;

    const hash = crypto
      .createHash('sha246')
      .update(url)
      .digest('base64')
      .substr(0, 5);

    const client = clients[hr.get(hash)];

    client.query('INSERT INTO url_table(url_id,url) VALUES ($1,$2)', [
      hash,
      url,
    ]);

    res.status(200).send({ url_id: hash, message: 'url added successfully' });
  } catch (error) {
    console.log('error', error);
    res.status(500).send({ error: 'Failed to add URL' });
  }
});

async function connect() {
  try {
    await clients.shard1.connect();
    await clients.shard2.connect();
    await clients.shard3.connect();
    console.log('Database connections established.');
    app.listen(4000, () => console.log('server is running on port 4000'));
  } catch (error) {
    console.log('Error from database connection: ', error);
  }
}
