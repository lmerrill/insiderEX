const { GoogleSpreadsheet } = require('google-spreadsheet');
const { BigQuery } = require('@google-cloud/bigquery');
const creds = require('../service.json');

// Google Sheet configuration
const SOURCE_SPREADSHEET_ID = '1HJfSFQ4ldp7cg5gan3QDN4YFy47Es7Uxk8ZFP8Uv-pw';
const SOURCE_SHEET_NAME = 'New Weekly Deals';
const DEST_SPREADSHEET_ID = '1Rf8ZzdffbFXQFaqGFkLMmnqeXPMlyk1e7RHq1FdhzgM';
const DEST_SHEET_NAME = 'EmailSheet';
const KIBO_SHEET_ID = '10VGkVg-bLMGm3MZRH4X2viz-RMCv2kctR3k9NwjvbzQ';
const KIBO_SHEET_NAME = 'Kibo vs JDA';
const DATE_REGEX = /\b(\d{1,2}\/\d{1,2}\/\d{2})\b(?:\s*-\s*(\d{1,2}\/\d{1,2}\/\d{2}))?/;

// BigQuery configuration
const bigquery = new BigQuery({
  keyFilename: './bigquery.json', // Path to your BigQuery service account key file
  projectId: 'phonic-spot-335716', // Replace with your Google Cloud Project ID
  clientOptions: {
    retry: {
      retryCodes: [503], // Retry on service unavailable errors
    },
  },
});

// Fetch data from Google Sheets
async function fetchSheetData(spreadsheetId, sheetName) {
  const doc = new GoogleSpreadsheet(spreadsheetId);
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();
  const sheet = doc.sheetsByTitle[sheetName];
  const rows = await sheet.getRows();
  return rows;
}

// Parse date fields
function parseDate(dateField) {
  const match = DATE_REGEX.exec(dateField);
  if (!match) return { Beg_DT: null, End_DT: null };
  const Beg_DT = match[1];
  const End_DT = match[2] || match[1];
  return { Beg_DT, End_DT };
}

// Process email sheet data
async function processEmailSheet(crazyDealsData, promoData, kiboData) {
  const rows = await fetchSheetData(SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME);
  let outputRows = [];
  let rowsRead = rows.length;

  // Load all cells for the sheet
  if (rows.length > 0) {
    await rows[0]._sheet.loadCells();
  }

  rows.forEach(row => {
    const backgroundColor = row._rawData[0]?.backgroundColor || row._sheet.getCell(row.rowIndex - 1, 0).backgroundColor; // Check the first cell's background color
    if (backgroundColor === '#b7e1cd') return; // Skip rows with the specified background color

    const dateField = row._rawData[0]; // Column 1 "Date"
    if (!dateField || !DATE_REGEX.test(dateField)) return; // Skip invalid date format

    const { Beg_DT, End_DT } = parseDate(dateField);

    // Exclude rows if Beg_DT is older than 3 days or more than validDaysOut days in the future
    const compDate = new Date(End_DT);
    const currentDate = new Date();
    const diffTime = compDate - currentDate; // Calculate the difference in milliseconds
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)); // Convert milliseconds to days

    if (diffDays < 0 || diffDays > validDaysOut) return;

    const days = dateField.split(' ')[0];
    const skus = row._rawData[2]
      ? row._rawData[2].split(/[, ]+/).map(sku => sku.trim()).filter(Boolean)
      : [];

    // Ensure "PRODUCT TYPE" is never blank (default to "tbd")
    const productType = row._rawData[10]?.trim() || 'tbd';

    if (
      skus.length === 0 ||
      productType.toLowerCase().includes('email only') ||
      productType.toLowerCase().includes('image collection')
    ) {
      return; // Skip empty SKU rows and specific product types
    }

    // Add your processing logic here
  });

  console.log(`Rows Processed: ${rowsRead}, Rows Written: ${outputRows.length}`);
}

// Query BigQuery with retries
async function queryBigQuery(sqlQuery) {
  let retries = 5;
  while (retries > 0) {
    try {
      // Run the query
      const [rows] = await bigquery.query({ query: sqlQuery });
      console.log(`Rows read from BigQuery: ${rows.length}`);
      return rows;
    } catch (err) {
      console.error('ERROR:', err);
      if (err.code === 503 && retries > 0) {
        console.log(`Retrying... (${5 - retries + 1}/5)`);
        retries--;
        await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait before retrying
      } else {
        console.error('Failed to query BigQuery after retries.');
        return [];
      }
    }
  }
}

// Execute the functions
async function main() {
  console.log('Crazy Deals Data:');
  const crazyDealsData = await queryBigQuery(getCrazyDeals);
  console.log('Event Data:');
  const promoData = await queryBigQuery(getEvents);
  console.log('Report Data:');
  const kiboData = await fetchSheetData(KIBO_SHEET_ID, KIBO_SHEET_NAME);

  await processEmailSheet(crazyDealsData, promoData, kiboData);
}

main().catch(err => console.error(err));