#! /usr/bin/env node

const dsv = require('d3-dsv')
const fs = require('fs')
// const rethinkdbdash = require('rethinkdbdash')
const r = require('rethinkdb')
const _ = require('lodash')
const pThrottle = require('p-throttle')

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
    limit: 50,
  },
})

// Read CSV.
const rows = dsv.csvParse(fs.readFileSync(argv.f, 'utf8'))

// const pace = require('pace')(rows.length)

// let r = rethinkdbdash()

const DB_NAME = 'geocode'
const TABLE_NAME = 'addresses'

const connect = () =>
  r.connect({ host: 'localhost', port: 28015 })

// Create db and table if not present.
const createDB = c =>
  new Promise((resolve, reject) => {
    r.dbList().run(c)
      .then(list => {
        if (!list.includes(DB_NAME)) {
          r.dbCreate(DB_NAME).run(c)
            .then(() => resolve(c))
            .catch(reject)
        } else {
          resolve(c)
        }
      })
      .catch(reject)
  })

const useTable = c =>
  new Promise((resolve, reject) => {
    c.use(DB_NAME)
    r.tableList().run(c)
      .then(list => {
        if (!list.includes(TABLE_NAME)) {
          r.tableCreate(TABLE_NAME).run(c)
            .then(() => resolve(c))
            .catch(reject)
        } else {
          resolve(c)
        }
      })
      .catch(reject)
  })

const geocode = ({ address, i }) =>
  new Promise((resolve, reject) => {
    googleMapsClient.geocode({ address }).asPromise()
      .then(geocode => {
        const status = _.get(geocode, 'json.status', '')
        if (status === 'OK' || status === 'ZERO_RESULTS') {
          resolve(geocode)
        } else {
          console.log(JSON.stringify(geocode, null, 2))
          reject(`${i} error: status ${status} for ${address}`)
        }
      }).catch(e => {
        console.error(e)
        console.error(`${i} error: could not geocode ${address}`)
        reject(e)
      })
  })

const processRow = ({ c, row, i }) =>
  new Promise((resolve, reject) => {

    const { address } = row
    r.table(TABLE_NAME).filter({ address }).run(c)
      .then(cursor => {
        cursor.toArray()
          .then(results => {

            if (results.length) {
              console.log(`${i} - found ${address} in db`)
              resolve(results[0])
            } else {
              geocode({ address, i }).then(geocode => {
                geocodeRequests++
                console.log(`      ${i} - geocoded ${address}`)
                const doc = { address, geocode }
                r.table(TABLE_NAME).insert(doc).run(c)
                  .then(() => {
                    console.log(`            ${i} - inserted ${address} in db`)
                    resolve(doc)
                  }).catch(e => {
                    console.error(e)
                    console.error('error trying to insert')
                    reject(e)
                  })
              }).catch(e => {
                console.error(e)
                console.error('error trying to geocode')
                reject(e)
              })
            }

          })
          .catch(e => {
            console.error(e)
            console.error('error trying to get cursor to array')
            reject(e)
          })


      }).catch(e => {
        console.error(e)
        console.error('error trying to find in db')
        reject(e)
      })

  })

connect()
  .then(createDB)
  .then(useTable)
  .then(c => {

    // const throttle = pThrottle((row, i) => processRow({ row, i, c }), 50, 1000)
    // const promises = rows.map(throttle)

    const promises = rows.map((row, i) => processRow({ row, i, c }))

    // const promises = [
    //   { address: 'N 7TH AVE & W VAN BUREN ST, Phoenix, TX' }
    // ].map(throttle)

    Promise.all(promises)
      .then(all => {

        const results = _(all)
          .map(d => ({
            address: d.address,
            coords: _.values(
              _.get(d, 'geocode.json.results[0].geometry.location', {})).join(',')
          }))
          .value()

        const filename = `geocoded-${argv.f}`
        fs.writeFileSync(filename, dsv.csvFormat(results))
        console.log(`Wrote ${all.length} records to ${filename}.`)
        console.log(`Hit Google ${geocodeRequests} times.`)
      })
      .catch(e => {
        console.error(e)
      })

  })
