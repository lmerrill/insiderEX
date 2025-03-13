const { GoogleSpreadsheet } = require('google-spreadsheet');
const { BigQuery } = require('@google-cloud/bigquery');
const creds = require('../service.json');

// Google Sheet configuration
//const SOURCE_SPREADSHEET_ID = '14j_5ZEGH6kJOnyKMOj0GLFhYLhRVrCZAEhZjRalvxYg'; Test
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
});
const projectId = 'noble-anvil-331720'; // This is needed also
const datasetId = 'lloydmerrill'; // Replace with your dataset ID
const viewId = 'v_cd_data'; // Replace with your view ID
const promoViewId = 'v_DW_Promotions_Prod_IX'; // New BigQuery view for promotional events

// Get the runtime parameter for daysOut
const daysOutArg = process.argv[2];
const daysOut = parseInt(daysOutArg, 10);
const validDaysOut = !isNaN(daysOut) && daysOut > 0 ? daysOut : 10;

async function fetchSheetData(spreadsheetId, sheetName) {
    const doc = new GoogleSpreadsheet(spreadsheetId);
    await doc.useServiceAccountAuth(creds);
    await doc.loadInfo();
    const sheet = doc.sheetsByTitle[sheetName];
    const rows = await sheet.getRows();
    return rows;
}

function parseDate(dateStr) {
    const match = DATE_REGEX.exec(dateStr);
    if (!match) return null;

    const [_, begDate, endDate] = match;
    return {
        Beg_DT: formatDate(begDate),
        End_DT: formatDate(endDate || begDate) // If no end date, duplicate start date
    };
}

function formatDate(dateStr) {
    const [month, day, year] = dateStr.split('/').map(Number);
    return `20${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

const rangeRegex = /Buy for \$?(\d{1,7}(?:\.\d{2})?)-\$?(\d{1,7}(?:\.\d{2})?) and get a Crazy Deal Gift Card of Equal Value/i;
const singleRegex = /Buy for \$(\d{1,7}(?:\.\d{2})?)[^\d]+\$(\d{1,7}(?:\.\d{2})?)/i;

function parseCD(text) {
    if (!text) return null;

    const matchRange = text.match(rangeRegex);
    if (matchRange) {
        return {
            buy: `${matchRange[1]}-${matchRange[2]}`,
            get: `${matchRange[1]}-${matchRange[2]}`
        };
    }

    const match = text.match(singleRegex);
    if (match) {
        const formatPrice = (num) => num.includes('.') ? num : `${num}.00`;

        return {
            buy: formatPrice(match[1]),
            get: formatPrice(match[2])
        };
    }

    return null; // Return null if the pattern is not found
}

async function writeToSheet(data, rowsRead) {
    const doc = new GoogleSpreadsheet(DEST_SPREADSHEET_ID);
    await doc.useServiceAccountAuth(creds);
    await doc.loadInfo();
    let sheet = doc.sheetsByTitle[DEST_SHEET_NAME];

    // If the sheet doesn't exist, create it
    if (!sheet) {
        sheet = await doc.addSheet({ title: DEST_SHEET_NAME });
    } else {
        await sheet.clear(); // Clear existing content
    }

    // Set header row
    await sheet.setHeaderRow(['Days', 'Beg_DT', 'End_DT', 'SKU(s)', 'PRODUCT / COLLECTION NAME', 'Active', 'PRODUCT TYPE', 'start_date', 'end_date', 'MMID', 'Buy', 'Get', 'promotion_id', 'sale_price', 'issue']);

    // Append new data
    if (data.length > 0) {
        await sheet.addRows(data);
    }

    console.log(`Rows Processed: ${rowsRead}, Rows Written: ${data.length}`);
}

async function processEmailSheet(crazyDealsData, promoData, kiboData) {
    const rows = await fetchSheetData(SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME);
    let outputRows = [];
    let rowsRead = rows.length;

    rows.forEach(row => {
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
        const skus = row._rawData[3] ? row._rawData[3].split(',').map(sku => sku.trim()).filter(Boolean) : [];

        // Ensure "PRODUCT TYPE" is never blank (default to "tbd")
        const productType = row._rawData[11]?.trim() || 'tbd';

        if (skus.length === 0 || productType.toLowerCase() === 'email only') return; // Skip empty SKU rows and 'EMAIL ONLY' product types

        skus.forEach(sku => {
            const kiboMatch = kiboData.find(kiboRow => kiboRow.Kibo_SKU && kiboRow.Kibo_SKU.toString() === sku.toString());
            const activeValue = kiboMatch ? kiboMatch.Kibo_IsActive.toString() : 'not-Web'; // Use the Kibo_IsActive field for the Active value
            const issue = ''; // Placeholder for issue field            
            if (row._rawData[7] === 'N/A') {
                const parsedSku = parseInt(sku);
                const matchingPromo = promoData.find(promo => {
                    const promoStartDate = formatDateFromBigQuery(promo.start_date);
                    const promoEndDate = formatDateFromBigQuery(promo.end_date);
                    return promo.item_id === parsedSku && promoStartDate === Beg_DT && promoEndDate === End_DT;
                });
                if (matchingPromo) {
                    outputRows.push({
                        Days: days,
                        Beg_DT,
                        End_DT,
                        'SKU(s)': sku,
                        'PRODUCT / COLLECTION NAME': row._rawData[4],
                        Active: activeValue,
                        'PRODUCT TYPE': productType,
                        start_date: 'na',
                        end_date: 'na',
                        MMID: 'na',
                        Buy: 'na',
                        Get: 'na',
                        promotion_id: matchingPromo.promotion_id.toString(),
                        sale_price: matchingPromo.sale_price.toString(),
                        issue:  'N'
                    });
                } else {
                    outputRows.push({
                        Days: days,
                        Beg_DT,
                        End_DT,
                        'SKU(s)': sku,
                        'PRODUCT / COLLECTION NAME': row._rawData[4],
                        Active: activeValue,
                        'PRODUCT TYPE': productType,
                        start_date: 'na',
                        end_date: 'na',
                        MMID: 'na',
                        Buy: 'na',
                        Get: 'na',
                        promotion_id: 'missing',
                        sale_price: 'missing',
                        issue: 'Y'
                    });
                }
            } else {
                const matchingDeal = crazyDealsData.find(deal => deal.item_id === sku);
                const { buy, get } = parseCD(row._rawData[7]) || { buy: 'missing', get: 'missing' };

                if (matchingDeal) {
                    outputRows.push({
                        Days: days,
                        Beg_DT,
                        End_DT,
                        'SKU(s)': sku,
                        'PRODUCT / COLLECTION NAME': row._rawData[4],
                        Active: activeValue,
                        'PRODUCT TYPE': productType,
                        start_date: formatDateFromBigQuery(matchingDeal.start_date),
                        end_date: formatDateFromBigQuery(matchingDeal.end_date),
                        MMID: matchingDeal.MMID.toString(),
                        Buy: matchingDeal.minimum_purchase,
                        Get: ((matchingDeal.minimum_purchase * matchingDeal.crazy_deal_percent) / 100.00).toFixed(2),
                        promotion_id: 'na',
                        sale_price: 'na',
                        issue: 'N'
                    });
                } else {
                    outputRows.push({
                        Days: days,
                        Beg_DT,
                        End_DT,
                        'SKU(s)': sku,
                        'PRODUCT / COLLECTION NAME': row._rawData[4],
                        Active: activeValue,
                        'PRODUCT TYPE': productType,
                        start_date: 'missing',
                        end_date: 'missing',
                        MMID: 'missing',
                        Buy: buy,
                        Get: get,
                        promotion_id: 'na',
                        sale_price: 'na',
                        issue: 'Y'
                    });
                }
            }
        });
    });

    await writeToSheet(outputRows, rowsRead);
}

function formatDateFromBigQuery(dateValue) {
  if (!dateValue) return 'missing';
  if (typeof dateValue === 'string') return dateValue; // Already a string
  if (dateValue.value) return dateValue.value; // BigQueryDate format
  if (dateValue.toISOString) return dateValue.toISOString().split('T')[0]; // Convert Date object to YYYY-MM-DD
  return 'missing'; // Fallback for unexpected types
}

async function queryBigQueryView(viewId) {
  try {
    // Construct SQL query
    const query = `
      SELECT *
      FROM \`${projectId}.${datasetId}.${viewId}\`
    `;

    // Run the query
    const [rows] = await bigquery.query({ query });

    console.log(`Rows read from BigQuery: ${rows.length}`);

    return rows;
  } catch (err) {
    console.error('ERROR:', err);
    return [];
  }
}

// Execute the functions
async function main() {
    const crazyDealsData = await queryBigQueryView(viewId);
    const promoData = await queryBigQueryView(promoViewId);
    const kiboData = await fetchSheetData(KIBO_SHEET_ID, KIBO_SHEET_NAME);
    await processEmailSheet(crazyDealsData, promoData, kiboData);
}

main().catch(console.error);