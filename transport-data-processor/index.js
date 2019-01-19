const config = require('./lib/config');
const csv = require('fast-csv');
const fs = require('fs');
const request = require('request');
const carparkIntervals = config.get('intervals:carparks')
const trafficIntervals = config.get('intervals:trafficImages')

const express = require('express'),
    app = express(),
    port = process.env.PORT || 3000;

const isFirstTimeLoad = config.get('isFirstTimeLoad');
if (!isFirstTimeLoad) {
    (function schedule() {
        fetchCarparkAvailability(() => {
            console.log('Process finished, waiting %d minutes', carparkIntervals.inMinutes);
            setTimeout(() => {
                console.log('Going to fetch data again');
                schedule();
            }, 1000 * 60 * carparkIntervals.inMinutes);
        });
    })();
}

let traficCameraInfo = {};
let isTrafficDataLoaded = false;

(function loadTraficCameraInfo() {
    fs.readFile('data/trafic_camera_info.json', 'utf8', function (err, data) {
        if (!err) {
            traficCameraInfo = JSON.parse(data);
            isTrafficDataLoaded = true;
        }
        console.log('Successfully loaded trafic camera infomation to memory', Object.keys(traficCameraInfo).length);
    });
})();

(function schedule() {
    fetchAndSyncTrafficImages(() => {
        console.log('Process finished, waiting %d minutes', trafficIntervals.inMinutes);
        setTimeout(() => {
            console.log('Going to fetch data again');
            schedule();
        }, 1000 * 60 * trafficIntervals.inMinutes);
    });
})();

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
        const url = config.get('cloudFunctions:baseUrl') + config.get('cloudFunctions:uploadCarparkInfoEndpoint');
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
    const url = config.get('sgDataGov:baseUrl') + config.get('sgDataGov:carparkAvailablityEndpoint');
    request.get(url, { json: true },
        (err, resp, data) => {
            if (err || !data.items || data.items.length == 0) {
                console.error('Error while fetching Carpark Availablity', err, data);
                done();
            } else {
                console.log('Number of car park records received', data.items[0].carpark_data.length);
                uploadCarparkAvailability(data.items[0].carpark_data, done);
            }
        }
    );
}

function uploadCarparkAvailability(carparkInfo, done) {
    let pageSize = 2000;
    const url = config.get('cloudFunctions:baseUrl') + config.get('cloudFunctions:uploadCarparkAvailabilityEndpoint');
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
            }, 1000 * carparkIntervals.inSeconds);
        } else {
            console.log('Finished uploading data');
            done();
        }
    });
}

function fetchAndSyncTrafficImages(done) {
    if (!isTrafficDataLoaded) {
        setTimeout(() => { done() });
        return;
    }

    const url = config.get('sgDataGov:baseUrl') + config.get('sgDataGov:traficImageEndpoint');
    request.get(url, { json: true },
        (err, resp, data) => {
            if (err || !data.items || data.items.length == 0) {
                console.error('Error while fetching traffic images', err, data);
                done();
            } else if (data.items[0].cameras.length > 0) {
                let cameras = data.items[0].cameras;
                let camerasWithoutGeoInfo = [];
                console.log('Number of trafic image records received', cameras.length);
                for (let camera of cameras) {
                    let cameraInfo = traficCameraInfo[camera.camera_id];
                    if (!cameraInfo) {
                        if (camera.location && camera.location.latitude && camera.location.longitude) {
                            camerasWithoutGeoInfo.push(camera);
                            traficCameraInfo[camera.camera_id] = {
                                camera_id: camera.camera_id,
                                timestamp: camera.timestamp,
                                image: camera.image,
                                latitude: camera.location.latitude,
                                longitude: camera.location.longitude,
                            };
                        }
                    } else {
                        cameraInfo.timestamp = camera.timestamp;
                        cameraInfo.image = camera.image;
                        if (camera.location && camera.location.latitude && camera.location.longitude) {
                            if (camera.location.latitude !== cameraInfo.latitude
                                || camera.location.longitude !== cameraInfo.longitude) {
                                camerasWithoutGeoInfo.push(camera);
                                cameraInfo.latitude = camera.location.latitude;
                                cameraInfo.longitude = camera.location.longitude;
                            }
                        }
                    }
                }
                if (camerasWithoutGeoInfo.length > 0) {
                    fetchAndUpdateReverseGeoCodingInfo(camerasWithoutGeoInfo, () => {
                        uploadTraficImages(Object.values(traficCameraInfo), done);
                    });
                } else {
                    uploadTraficImages(Object.values(traficCameraInfo), done);
                }
            } else {
                console.error("No traffic image data found");
                done();
            }
        }
    );
}

function uploadTraficImages(cameras, done) {
    let pageSize = 100;
    const url = config.get('cloudFunctions:baseUrl') + config.get('cloudFunctions:uploadTrafficImagesEndpoint');
    uploadData(cameras, 'traffic_images', url, pageSize, 0, done);
}

function fetchAndUpdateReverseGeoCodingInfo(cameras, done) {
    let receiveAllRecords = () => {
        writeToFile('data/trafic_camera_info.json', traficCameraInfo, done);
    }
    let receivedOneRecord = (err, camera_id, data) => {
        if (err) {
            console.error('Error while fetching reverse geocoding data', err, data);
        } else {
            let cameraInfo = traficCameraInfo[camera_id];
            cameraInfo.address = data.address;
            if (cameras.length > 0) {
                setTimeout(() => {
                    console.log('Going to fetch geocoding data again as no of items : ' + cameras.length);
                    fetchReverseGeoCodingInfo(cameras.pop(), receivedOneRecord);
                }, 1000 * trafficIntervals.inSeconds);
            } else {
                receiveAllRecords();
            }
        }
    }
    fetchReverseGeoCodingInfo(cameras.pop(), receivedOneRecord);
}

function fetchReverseGeoCodingInfo(camera, done) {
    const url = config.get('openStreetMap:baseUrl') + config.get('openStreetMap:reverseGeoCodingEndpoint');
    request.get(url,
        {
            qs: {
                format: 'json',
                zoom: 18,
                addressdetails: 1,
                lat: camera.location.latitude,
                lon: camera.location.longitude
            },
            json: true,
            headers: {
                'User-Agent': 'SGTravelBuddy'
            }
        },
        (err, resp, data) => done(err, camera.camera_id, data)
    );
}

function writeToFile(file, content, done) {
    fs.writeFile(file, JSON.stringify(content), 'utf8', done);
}
