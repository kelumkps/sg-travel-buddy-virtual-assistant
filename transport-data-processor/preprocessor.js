const csv = require("fast-csv");
const request = require("request");
const fs = require('fs');

csv.fromPath("data/hdb-carpark-information.csv", { headers: true, ignoreEmpty: true })
    .transform((row, callback) => {
        convertToGPSCoordinates(row.x_coord, row.y_coord, (err, resp) => {
            if (err) console.log(err);
            row["latitude"] = resp.body.latitude;
            row["longitude"] = resp.body.longitude;
            callback(err, row);
        });
    })
    .pipe(csv.createWriteStream({ headers: true }))
    .pipe(fs.createWriteStream("data/out.csv", { encoding: "utf8" }));


function convertToGPSCoordinates(x, y, callback) {
    const conversionUrl = "https://developers.onemap.sg/commonapi/convert/3414to4326";
    request.get(conversionUrl,
        {
            qs: { "X": x, "Y": y },
            json: true
        },
        callback
    );
}

