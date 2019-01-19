const config = require('./lib/config');
const urlUtil = require('url');
const csv = require('fast-csv');
const request = require('request');
const intervalInSeconds = process.env.INTERVAL_IN_SECONDS || config.get('intervals:inSeconds') || 30;
const intervalInMinutes = process.env.INTERVAL_IN_MINUTES || config.get('intervals:inMinutes') || 5;
const express = require('express'),
    app = express(),
    port = process.env.PORT || 3000;

const isFirstTimeLoad = config.get('isFirstTimeLoad');
if (!isFirstTimeLoad) {
    (function schedule() {
        fetchCarparkAvailability(() => {
            console.log('Process finished, waiting %d minutes', intervalInMinutes);
            setTimeout(() => {
                console.log('Going to fetch data again');
                schedule();
            }, 1000 * 60 * intervalInMinutes);
        });
    })();
}

app.listen(port, () => {
    console.log('SGTravelBuddy Action for Google Assistant is started on: ' + port);
});

app.get('/', function (req, res) {
    res.send('SGTravelBuddy Actions for Google Assistant');
});

app.get('/api', function (req, res) {
    res.send('API is running. Current Time: ' + (new Date()).toISOString());
});

app.get('/api/loadCarparks', (req, res, next) => {
    loadCarparkInfo(carparkInfo => {
        let pageSize = req.query.pageSize || 500;
        const url = urlUtil.resolve(config.get('cloudFunctions:baseUrl'), config.get('cloudFunctions:uploadCarparkInfoEndpoint'));
        uploadData(carparkInfo, 'carparks', url, pageSize, 0, () => {
            res.send('Loading carparks is completed');
        });
    })
});

function loadCarparkInfo(done) {
    carparkInfo = [];
    csv.fromPath('data/out.csv', { headers: true, ignoreEmpty: true })
        .on("data", function (data) {
            carparkInfo.push(data);
        })
        .on("end", function () {
            console.log('Successfully loaded carpark infomation to memory', carparkInfo.length);
            done(carparkInfo);
        });
}

function fetchCarparkAvailability(done) {
    const url = urlUtil.resolve(config.get('sgDataGov:baseUrl'), config.get('sgDataGov:carparkAvailablityEndpoint'));
    request.get(url, { json: true },
        (err, resp, data) => {
            if (err || !data.items || data.items.length == 0) {
                console.error('Error while fetching Carpark Availablity', err, data);
                done();
            } else {
                console.log('Number of records received', data.items[0].carpark_data.length);
                uploadCarparkAvailability(data.items[0].carpark_data, done);
            }
        }
    );
}

function uploadCarparkAvailability(carparkInfo, done) {
    let pageSize = 2000;
    const url = urlUtil.resolve(config.get('cloudFunctions:baseUrl'), config.get('cloudFunctions:uploadCarparkAvailabilityEndpoint'));
    uploadData(carparkInfo, 'carpark_data', url, pageSize, 0, done);
}

function getArrayFragment(items, pageSize = 10, offset = 0) {
    return {
        data: items.slice(offset, offset + pageSize),
        nextPage: offset +
            pageSize < items.length ? offset + pageSize : undefined
    }
}

function uploadData(items, field, url, pageSize, offset, done) {
    let arrayFragment = getArrayFragment(items, pageSize, offset);
    let requestBody = {};
    requestBody[field] = arrayFragment.data;
    request.post({
        url: url,
        json: requestBody
    }, (err, resp, data) => {
        console.log('Next Page', arrayFragment.nextPage, 'response', data, 'error', err);
        if (arrayFragment.nextPage) {
            setTimeout(() => {
                console.log('Going to send data again');
                uploadData(items, field, url, pageSize, arrayFragment.nextPage, done);
            }, 1000 * intervalInSeconds);
        } else {
            console.log('Finished uploading data');
            done();
        }
    });
}
