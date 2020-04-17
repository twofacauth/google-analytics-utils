/**
* ga_to_bigquery.gs
*
* Copyright (c) 2020 Takamichi Yanai
*
* Released under the MIT license.
* see https://opensource.org/licenses/MIT
*
* Retrieves Google Analytics report data via API and sends them to Bigquery
*/

/**
* Config
*/
const today = new Date()
const strYesterday = Utilities.formatDate(new Date(today.getTime() - 1 * 24 * 60 * 60 * 1000),
                                          Session.getScriptTimeZone(), 'yyyyMMdd')
// GCP setting
const projectId = 'my-project-id'
const datasetId = 'temp_analysis'
const tableId = 'ga_' + strYesterday
// Google Analytics setting
const profileId = 99999999
const maxResults = 100000
const request = {
  'viewId': 'ga:' + profileId,
  'dateRanges': [{
//    'startDate': '12daysAgo',
    'startDate': 'yesterday',
    'endDate':'yesterday'
  }],
  'samplingLevel': 'LARGE',
  'pageSize': maxResults,
  'metrics':[
    { 'expression':'ga:pageviews' },
    { 'expression':'ga:entrances' }
  ],
  'dimensions': [
    { 'name':'ga:sourceMedium' },
    { 'name':'ga:deviceCategory' },
    { 'name':'ga:pagePath' },
    { 'name':'ga:clientId' },
    { 'name':'ga:dimension3' },
    { 'name':'ga:sessionCount' },
    { 'name':'ga:date' },
    { 'name':'ga:dimension5' },
    { 'name':'ga:dimension7' }
  ]
}

/**
* Runs a report by ReportRequest Object.
* @param {Object} reportRequest - ReportRequest Object below.
* https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
*/
function runReportV4(reportRequest) {
  let reportRequests = {'reportRequests': [reportRequest]}
  let fields, data =[] 
  while(1) {
    let response, report, rows
    try {
      response = AnalyticsReporting.Reports.batchGet(reportRequests)
    } catch(e) {
      Logger.log("API call failed: " + e.message)
      throw "API call failed"
    }
    try {
      report = response['reports'][0]
      rows = report['data']['rows']
    } catch(e) {
      throw "Response data is empty."
    }
    fields = report['columnHeader']['dimensions'].map(x=>{return {name: x.replace('ga:', ''), type: 'string'}})
    let metrics = report['columnHeader']['metricHeader']['metricHeaderEntries'].map(x=>{return {name: x['name'].replace('ga:', ''), type: x['type']}})
    fields = fields.concat(metrics)
    data = data.concat(rows.map(row=>{return row['dimensions'].concat(row['metrics'][0]['values'])}))
    if (report['nextPageToken']) {
      reportRequests['reportRequests'][0]['pageToken'] = report['nextPageToken']
    } else {
      break
    }
  }
  return({
    fields: fields,
    data: data
  })
}

/**
* Loads a CSV into BigQuery
* @param {Object[]} fields - BigQuery column definition.
* @param {string} fields[].name - column name
* @param {string} fields[].type - column type INTEGER|STRING|FLOAT|BOOLEAN
* @param {(number|string)[][]} data - data to load
* @param {string} projectId - GCP project id
* @param {string} datasetId - BigQuery dataset name
* @param {string} tableId - BigQuery table name
*/
function loadCsv(fields, data, projectId, datasetId, tableId) {
  // Load CSV data from Drive and convert to the correct format for upload.
  let csv = data.map(row => '"' + row.join('","') + '"').join("\n");
  let blob = Utilities.newBlob(csv, 'text/csv');
  
  // Create the data upload job.
  let job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        sourceFormat: 'CSV',
        skipLeadingRows: 0,
        writeDisposition: 'WRITE_TRUNCATE',
        schema: {
          fields: fields
        },
        createDisposition: 'CREATE_IF_NEEDED',
      }
    }
  };
  job = BigQuery.Jobs.insert(job, projectId, blob);
}

// Get a report data and send to BigQuery.
function myFunction() {
  let {fields, data} = runReportV4(request)
  loadCsv(fields, data, projectId, datasetId, tableId)
  Logger.log("Finished: " + data.length + " rows.")
}

// Sends an email with the report CSV file.
function myFunction2() {
  let {fields, data} = runReportV4(request)
  const emailConfig = {
    sendTo: 'dummy@example.com',
    subject: 'Google Analytics Report('+ Utilities.formatDate(today, Session.getScriptTimeZone(), 'yyyy/MM/dd HH:mm') +')',
    body: 'Report CSV file is attached.'
    filename: 'report.csv'
  }
  let csv = fields.map(x => x.name).join(',') + "\n" + 
            data.map(row => '"' + row.join('","') + '"').join("\n")
  MailApp.sendEmail(
    emailConfig['sendTo'],
    emailConfig['subject'],
    emailConfig['body'],
    { attachments : [Utilities.newBlob(csv, 'text/csv').setName(emailConfig['filename'])] }
  )
}
