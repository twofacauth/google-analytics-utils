/**
* bigquery_to_mp.gs
*
* Copyright (c) 2020 Takamichi Yanai
*
* Released under the MIT license.
* see https://opensource.org/licenses/MIT
*
* The runQuery function is:
* Apache License 2.0 | https://github.com/gsuitedevs/apps-script-samples/blob/master/LICENSE
*
* Sends Google Analytics measurement protocol from data stored in Bigquery
*/

/**
* Config
*/
const projectId = 'my-project-name'
const query = 'select * from `crmdata.leads` limit 100;'
const sleepTimeMs = 500;
const strUa = 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0'
const strUrl = 'https://www.google-analytics.com/collect'
//const strUrl = 'https://www.google-analytics.com/debug/collect' // debug
const oParamsCommon = {
  'v': 1,
  'tid': 'UA-99999999-1',
  't': 'event',
  'ec': 'mp_import',
  'ea': 'crm',
  'ni': 1
}

/**
* Runs a BigQuery query.
* @param {string} query - query string
* @param {string} projectId - GCP project id
*/
function runQuery(query, projectId) {
  let request = {
    query : query,
    useLegacySql: false,
//    maxResults: 1000000
  }
  let queryResults = BigQuery.Jobs.query(request, projectId)
  let jobId = queryResults.jobReference.jobId

  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }
  let rows = queryResults.rows;

  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }

  if (rows) {
    let headers = queryResults.schema.fields.map(field => field.name)
    let objRecords = rows.map(row=>{
      let obj = {}
      row.f.map((col, i)=>{obj[headers[i]] = col.v})
      return obj
    })
    return objRecords
  } else {
    Logger.log('No rows returned.');
  }

}

function myFunction() {
  result = runQuery(query, projectId)
  result.map((row, i)=>{
    let oParams = Object.assign(row, oParamsCommon)
    try {
      let response = UrlFetchApp.fetch(strUrl, {
        method: 'post',
        headers: {
          'User-Agent': strUa // When user-agent is not specified , the hit is not counted.
        },
        payload: oParams,
        muteHttpExceptions: false
      })
      Logger.log(oParams)
      Logger.log(`Line ${i}: succeeded.`)
    } catch(e) {
      Logger.log(`Line ${i}: ${e}`)
    }
  })
}
