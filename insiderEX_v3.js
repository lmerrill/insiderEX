const { GoogleSpreadsheet } = require('google-spreadsheet');
const { BigQuery } = require('@google-cloud/bigquery');
const creds = require('../service.json');

// Google Sheet configuration
//const SOURCE_SPREADSHEET_ID = '14j_5ZEGH6kJOnyKMOj0GLFhYLhRVrCZAEhZjRalvxYg'; Test
const SOURCE_SPREADSHEET_ID = '1HJfSFQ4ldp7cg5gan3QDN4YFy47Es7Uxk8ZFP8Uv-pw';
const SOURCE_SHEET_NAME = 'New Weekly Deals';
//const DEST_SPREADSHEET_ID = '1Rf8ZzdffbFXQFaqGFkLMmnqeXPMlyk1e7RHq1FdhzgM';
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
      maxRetries: 5, // Maximum number of retries
      retryDelayMultiplier: 2, // Exponential backoff multiplier
    },
  },
});
const projectId = 'noble-anvil-331720'; // This is needed also

// Embedded SQL queries
const getEvents = `
  SELECT
    cast(d.item_id as int) as item_id,
    item.sales_channel,
    h.promotion_id,
    h.type,
    item.regular_price + d.discount_amount AS sale_price,
    h.start_date,
    h.end_date
  FROM
    \`phonic-spot-335716.core.dim_promotion\` h
  LEFT JOIN
    \`phonic-spot-335716.core.dim_promotion_item\` d
  ON
    h.promotion_sk = d.promotion_sk
  LEFT JOIN
    \`phonic-spot-335716.core.dim_item\` item
  ON
    d.item_id = item.item_id
  WHERE
    h.dbt_valid_to IS NULL
    AND d.dbt_valid_to IS NULL
    AND item.dbt_valid_to IS NULL
    AND not h.is_location_specific
    and h.start_date >= '2024-01-01'
    AND h.end_date >= Current_date()
    and h.type = 'Insider Exclusive'
    AND (d.discount_amount IS NOT NULL and d.discount_amount <> 0)
  order by cast(d.item_id as int),
           h.promotion_id
`;

const getCrazyDeals = `
  SELECT
    d.item_id,
    h.is_insider_exclusive,
    h.start_date,
    h.end_date,
    h.promotion_id as MMID,
    h.minimum_purchase,
    h.crazy_deal_percent,
    h.maximum_purchase
  FROM
    \`phonic-spot-335716.core.dim_promotion\` h
  LEFT JOIN
    \`phonic-spot-335716.core.dim_promotion_item\` d
  ON
    h.promotion_sk = d.promotion_sk
  LEFT JOIN
    \`phonic-spot-335716.core.dim_item\` item
  ON
    d.item_id = item.item_id
  WHERE
    h.dbt_valid_to IS NULL
    AND d.dbt_valid_to IS NULL
    AND item.dbt_valid_to IS NULL
    AND h.end_date >= current_date
    AND h.type IN ('Crazy Deal')
    AND h.promotion_id NOT IN ('220')
  Order by d.item_id
`;

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

    // Remove basic filter from the sheet before processing
    if (sheet.basicFilter) {
        await sheet.clearBasicFilter(); // Removes the basic filter
    }

    // Set header row
    await sheet.setHeaderRow(['Days', 'Beg_DT', 'End_DT', 'SKU(s)', 'PRODUCT / COLLECTION NAME', 'Active', 'PRODUCT TYPE', 'start_date', 'end_date', 'MMID', 'Buy', 'Get', 'promotion_id', 'sale_price', 'issue']);
    // Filter data to include only records with "Y" in the issue field
    data = data.filter(row => row.issue === 'Y');
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

        // Load cells for the range of rows and the first column
        if (rows.length > 0) {
          await rows[0]._sheet.loadCells({
              startRowIndex: 0,
              endRowIndex: rows.length,
              startColumnIndex: 0,
              endColumnIndex: 1, // Only load the first column (index 0)
          });
      }
  
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
        // Exclude rows where INSIDER PRICE = N/A and CRAZY DEAL STATUS = N/A
        //  These rows are being promoted at regular price
        // Important to trim spaces as some cells have trailing spaces
        if (row._rawData[7]?.trim() === 'N/A' && row._rawData[11]?.trim() === 'N/A') {
            return;
        // Access the backgroundColor using getCell()
        const backgroundColor = row._sheet.getCell(row.rowIndex - 1, 0).backgroundColor;
        if (backgroundColor === '#b7e1cd') return; // Skip rows with the specified background color

        }
        skus.forEach(sku => {
            const kiboMatch = kiboData.find(kiboRow => kiboRow.Kibo_SKU && kiboRow.Kibo_SKU.toString() === sku.toString());
            const activeValue = kiboMatch ? kiboMatch.Kibo_IsActive.toString() : 'not-Web'; // Use the Kibo_IsActive field for the Active value
            const issue = ''; // Placeholder for issue field            
            if (row._rawData[6]?.trim() === 'N/A') {
                const parsedSku = parseInt(sku);
                const matchingPromo = promoData.find(promo => {
                    const promoStartDate = formatDateFromBigQuery(promo.start_date);
                    const promoEndDate = formatDateFromBigQuery(promo.end_date);
/*                    return promo.item_id === parsedSku && promoStartDate === Beg_DT && promoEndDate === End_DT; */
                    return promo.item_id === parsedSku &&
                           promoStartDate <= Beg_DT &&
                           promoEndDate >= End_DT;
                });
                if (matchingPromo) {
                    outputRows.push({
                        Days: days,
                        Beg_DT,
                        End_DT,
                        'SKU(s)': sku,
                        'PRODUCT / COLLECTION NAME': row._rawData[3],
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
                        'PRODUCT / COLLECTION NAME': row._rawData[3],
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
                        'PRODUCT / COLLECTION NAME': row._rawData[3],
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
                        'PRODUCT / COLLECTION NAME': row._rawData[3],
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

async function queryBigQuery(sqlQuery) {
  let retries = 5; // Maximum retries
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

main().catch(console.error);