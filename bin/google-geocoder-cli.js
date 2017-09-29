#! /usr/bin/env node

const dsv = require('d3-dsv')
const fs = require('fs')
const NeDB = require('nedb')
const promiseThrottle = require('p-throttle')
const _ = require('lodash')

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

// Setup Google Maps connection.
const googleMapsClient = require('@google/maps').createClient({
  clientId: argv.i,
  clientSecret: argv.k
})

// Open connection to NeDB in current path.
const db = new NeDB({ filename: 'geocode.nedb', autoload: true })

// Read CSV.
const rows = dsv.csvParse(fs.readFileSync(argv.f, 'utf8'))

const pace = require('pace')(rows.length)

const getCoords = response => {
  const coords = _.get(response, 'json.results[0].geometry.location')
  return _.values(coords).join(',')
}

// Geocode a row. Returns a Promise.
let geocodeRequests = 0
const geocodeRow = promiseThrottle(
  row => {
    const { address } = row

    pace.op()

    // Look up the address in NeDB.
    db.find({ address }, function (err, docs) {
      // If there is an error, reject the promise;
      if (err) {
        Promise.reject(err)
      } else if (docs.length) {
        // otherwise, if there is a match, resolve the promise;
        Promise.resolve({ ...row, geocode: getCoords(docs[0].response) })
      } else {
        // otherwise, geocode.

        googleMapsClient.geocode({ address }, function (err, response) {
          geocodeRequests++
          if (err) {
            Promise.reject(err)
          } else {
            db.insert({ address, response })
            Promise.resolve({ ...row, geocode: getCoords(response) })
          }
        })
      }
    })
  },
  50,
  1000
)

Promise.all(rows.map(geocodeRow))
  .then(all => {
    const filename = `geocoded-${argv.f}`
    fs.writeFileSync(filename, dsv.csvFormat(all))
    console.log(`Wrote ${all.length} records to ${filename}.`)
    console.log(`Hit Google ${geocodeRequests} times.`)
  })
  .catch(e => {
    console.error(e)
  })
