const fs = require("fs");
const { parse } = require("csv-parse");

const slcsp_file = "./slcsp.csv";
const zips_file = "./zips.csv";
const plans_file = "./plans.csv";

const slcsp_data = [];
const plans_data = [];
const zips_data = [];

async function dataParser(filename, dataArray) {
    return new Promise((resolve, reject) => {
        let data = [];
        fs.createReadStream(filename)
            .pipe(parse({
                delimiter: ",",
                columns: true,
                ltrim: true,
            }))
            .on("data", function (row) {
                getSelectData(filename, row, dataArray);
            })
            .on("error", function (error) {
                reject(error.message);
            })
            .on("end", function () {
                resolve(data);
            });
    });
}

function getSelectData(filename, row, dataArray) {
    if (filename === plans_file) {
        // Only store state, rate_area, and rate for silver plans
        if (row.metal_level === "Silver") {
            let select_data = { state: row.state, rate_area: row.rate_area, rate: row.rate };
            dataArray.push(select_data);
        }
    } else if (filename === slcsp_file) {
        dataArray.push(row);
    } else if (filename === zips_file) {
        // Cross-reference with slcsp_data; only store unique records with matching zipcodes
        const matchingSlcsp = slcsp_data.find(slcsp => slcsp.zipcode === row.zipcode);
        const uniqueRecords = new Set();

        if (matchingSlcsp) {
            const matchingPlans = plans_data.filter(plan => plan.state === row.state && plan.rate_area === row.rate_area);

            for (const plan of matchingPlans) {
                const key = `${row.zipcode}-${row.state}-${row.rate_area}-${plan.rate}`;

                // Check if the key is already in the set, if not, add the record
                if (!uniqueRecords.has(key)) {
                    let select_data = {
                        state: row.state,
                        rate_area: row.rate_area,
                        zip: row.zipcode,
                        rate: parseFloat(plan.rate).toFixed(2)  // TODO: would like this to be array of rates for easier sorting and to reduce total # of records
                    };

                    dataArray.push(select_data);

                    // Add the key to the set to mark it as processed
                    uniqueRecords.add(key);
                }
            }
        }
    }
}

async function processData(filename, dataArray) {
    try {
        await dataParser(filename, dataArray);
        console.log(dataArray); // TEST OUTPUT
    } catch (error) {
        console.error(error);
    }
}

// Ensure plans_data and slcsp_data are processed before zips_data
Promise.all([
    processData(plans_file, plans_data),
    processData(slcsp_file, slcsp_data),
]).then(() => processData(zips_file, zips_data));

// Next step is to find SLC silver plan rate and add to correct spot in slcsp_data before outputting
// Rate might be blank if zipcode in multiple rate areas, there are only 0 or 1 matches, or if all matches are at same rate
