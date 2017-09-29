#! /usr/bin/env node

const dsv = require('d3-dsv')
const fs = require('fs')
const NeDB = require('nedb')
const PromisePool = require('es6-promise-pool')

const argv = require('yargs')
  .usage('Geocode a CSV of addresses, using Google Maps API.')
  .describe('i', 'client ID')
  .alias('i', 'id')
  .describe('k', 'client crypto key')
  .alias('k', 'key')
  .describe('f', 'CSV of addresses')
  .alias('f', 'file')
  .help('h')
  .alias('h', 'help')
  .demand(['i', 'k', 'f']).argv

const googleMapsClient = require('@google/maps').createClient({
  clientId: argv.i,
  clientSecret: argv.k
})

// Open connection to NeDB in current path.
const db = new NeDB({ filename: 'geocode.nedb', autoload: true })

// Read CSV.
const rows = dsv.csvParse(fs.readFileSync(argv.f, 'utf8'))

// Geocode a row. Returns a Promise.
const geocodeRow = function (row) {
  return new Promise(function (resolve, reject) {
    const { address } = row

    // Look up the address in NeDB.
    db.find({ address }, function (err, docs) {
      // If there is an error, reject the promise;
      if (err) {
        reject(err)
      } else if (docs.length) {
        // otherwise, if there is a match, resolve the promise;
        resolve()
      } else {
        // otherwise, geocode.

        googleMapsClient.geocode({ address }, function (err, response) {
          console.log(`Geocoded ${address}`)
          if (err) {
            reject(err)
          } else {
            db.insert({ address, response })
            resolve()
          }
        })
      }
    })
  })
}

// Iterate over every row.
let index = 0
const promiseProducer = function () {
  if (index < rows.length) {
    return geocodeRow(rows[index++])
  } else {
    return null
  }
}

// Set the concurrency.
const concurrency = 1

// Create the promise pool,
const pool = new PromisePool(promiseProducer, concurrency)

// listen to rejection,
pool.addEventListener('rejected', e => {
  console.log(JSON.stringify(e.data.error, null, 2))
})

// and start.
pool
  .start()
  .then(() => {
    console.log('all done')
  })
  .catch(e => {
    console.error(e)
  })
