#! /usr/bin/env node

const dsv = require('d3-dsv')
const fs = require('fs')
const NeDB = require('nedb')
const _ = require('lodash')
const pThrottle = require('p-throttle')

const RATE = 50

const getCoords = geocode =>
  _.values(_.get(geocode, 'json.results[0].geometry.location', {})).join(',')

const getStatus = geocode => _.get(geocode, 'json.status', '')

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
let geocodeRequests = 0
const googleMapsClient = require('@google/maps').createClient({
  clientId: argv.i,
  clientSecret: argv.k,
  Promise,
  rate: {
    limit: RATE
  }
})

// Read CSV.
const rows = dsv.csvParse(fs.readFileSync(argv.f, 'utf8'))
const pace = require('pace')(rows.length)

// Connect to DB.
const db = new NeDB({ filename: argv.f + '.nedb', autoload: true })

const geocode = address =>
  new Promise((resolve, reject) => {
    googleMapsClient
      .geocode({ address })
      .asPromise()
      .then(geocode => {
        geocodeRequests++
        const status = getStatus(geocode)
        if (status === 'OK' || status === 'ZERO_RESULTS') {
          resolve(geocode)
        } else {
          reject(new Error(status))
        }
      })
      .catch(reject)
  })

const throttledGeocode = pThrottle(geocode, RATE, 1000)

const processRow = (row, i) =>
  new Promise((resolve, reject) => {
    const { address } = row
    db.find({ address }, (err, docs) => {
      if (err) {
        reject(err)
      } else if (docs.length) {
        pace.op()
        resolve(docs[0])
      } else {
        throttledGeocode(address)
          .then(geocode => {
            const doc = {
              address,
              coords: getCoords(geocode),
              status: getStatus(geocode)
            }
            db.insert(doc, e => {
              if (e) {
                reject(e)
              } else {
                pace.op()
                resolve(doc)
              }
            })
          })
          .catch(reject)
      }
    })
  })

const promises = rows.map(processRow)

db.ensureIndex({ fieldName: 'address' }, err => {
  if (err) {
    console.error(err)
  } else {
    Promise.all(promises).then(all => {
      const filename = `geocoded-${argv.f}`
      fs.writeFileSync(
        filename,
        dsv.csvFormat(all.map(d => _.omit(d, '_id')))
      )
      console.log(`Wrote ${all.length} records to ${filename}.`)
      console.log(`Hit Google ${geocodeRequests} times.`)
      console.log('Summary of geocode requests:')
      console.log(JSON.stringify(_.countBy(all, 'status'), null, 2))
    })
  }
})
