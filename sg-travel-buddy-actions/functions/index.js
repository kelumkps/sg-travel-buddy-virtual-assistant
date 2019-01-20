'use strict';

// Import the Dialogflow module and response creation dependencies
// from the Actions on Google client library.
const {
    dialogflow,
    BasicCard,
    Permission,
    Image,
    Carousel,
    Button,
    BrowseCarousel,
    BrowseCarouselItem,
    Suggestions
} = require('actions-on-google');

// Import the firebase-functions package for deployment.
const functions = require('firebase-functions');
const admin = require('firebase-admin');
const GeoFire = require('geofire');
const convert = require('convert-units');
const Promise = require('promise');
const moment = require("moment-timezone");

// Instantiate the Dialogflow client.
const app = dialogflow({ debug: true });

// Handle the Dialogflow intent named 'find_nearby_car_parks'.
app.intent('find_nearby_car_parks', (conv, { radius }) => {
    conv.data.radius = radius;
    // Asks the user's permission to know their name, for personalization.
    conv.contexts.delete('search_type_context');
    conv.contexts.set('search_type_context', 15, { searchType: 'car_parks' })
    conv.ask(new Permission({
        context: 'To search car parks near to you',
        permissions: 'DEVICE_PRECISE_LOCATION',
    }));
});

// Handle the Dialogflow intent named 'find_nearby_traffic_images'.
app.intent('find_nearby_traffic_images', (conv, { radius }) => {
    conv.data.radius = radius;
    // Asks the user's permission to know their name, for personalization.
    conv.contexts.delete('search_type_context');
    conv.contexts.set('search_type_context', 15, { searchType: 'traffic_images' })
    conv.ask(new Permission({
        context: 'To search traffic images near to you',
        permissions: 'DEVICE_PRECISE_LOCATION',
    }));
});

// Handle the Dialogflow intent named 'find_car_parks'. If user
// agreed to PERMISSION prompt, then boolean value 'permissionGranted' is true.
app.intent('receive_current_location', (conv, params, permissionGranted) => {
    if (permissionGranted) {
        // If the user accepted our request, store their location in
        const {
            coordinates
        } = conv.device.location;
        if (!conv.data.radius || !conv.data.radius.amount) {
            conv.data.radius = {
                amount: 1,
                unit: 'km'
            }
        }
        conv.data.latitude = coordinates.latitude;
        conv.data.longitude = coordinates.longitude;
        let radiusInMeters = convert(conv.data.radius.amount).from(conv.data.radius.unit).to('m');
        let limit = 10;
        let context = conv.contexts.get('search_type_context');
        const { searchType } = context.parameters;
        if (searchType === 'car_parks') {
            return displayCarparkResults(conv, radiusInMeters, limit, 0);
        } else if (searchType === 'traffic_images') {
            return displayTrafficImageResults(conv, radiusInMeters, limit, 0);
        }
    } else {
        // If the user denied our request, go ahead with the conversation.
        conv.close('Sorry, I could not figure out where you are without your permission.');
    }
});

// Handle the Dialogflow intent named 'load_more_results'.
app.intent('load_more_results', (conv) => {
    let loadMorecontext = conv.contexts.get('load_more_context');
    const { limit, nextSkip } = loadMorecontext.parameters;
    let searchTypecontext = conv.contexts.get('search_type_context');
    const { searchType } = searchTypecontext.parameters;
    let radiusInMeters = convert(conv.data.radius.amount).from(conv.data.radius.unit).to('m');
    if (nextSkip) {
        if (searchType === 'car_parks') {
            return displayCarparkResults(conv, radiusInMeters, limit, nextSkip);
        } else if (searchType === 'traffic_images') {
            return displayTrafficImageResults(conv, radiusInMeters, limit, nextSkip);
        }
    } else {
        conv.contexts.delete('load_more_context');
        conv.contexts.delete('search_type_context');
        let nextRadius = radiusInMeters * 2;
        let nextRadiusWithUnit = (nextRadius >= 1000) ? (Number(convert(nextRadius).from('m').to('km')).toFixed(2) + ' km') : (Number(nextRadius).toFixed() + ' m');
        conv.ask('Sorry, no more data to load. Please start a new search.');
        conv.ask(new Suggestions(['Find car parks', 'Show Traffic Images', 'Find car parks in ' + nextRadiusWithUnit, 'Show traffic images in ' + nextRadiusWithUnit]));
    }
});

// Handle the Dialogflow intent named 'show_single_traffic_image'.
app.intent('show_single_traffic_image', (conv, params, option) => {
    if (option) {
        return db.ref(traffic_collection_name + '/' + option)
            .once('value')
            .then((snapshot) => {
                let trafficImage = snapshot.val();
                let origin = [conv.data.latitude, conv.data.longitude];
                let destination = [trafficImage.latitude, trafficImage.longitude];
                trafficImage.distance = GeoFire.distance(origin, destination);
                conv.ask(`<speak><p><s>Here is the traffic image you requested.</s></p></speak>`);
                conv.ask(toTrafficImageAsBasicCard(trafficImage));
                conv.ask(new Suggestions(['Find car parks', 'Show traffic images', 'Find car parks in 500m', 'Find car parks in 5km', 'Traffic images in 5km']));
                return Promise.resolve();
            }).catch(err => {
                console.error('Error while fetching traffic image with key', option, err);
                if (err) conv.close("I'm sorry, but something went wrong. Please try again later.");
                return Promise.resolve();
            });
    } else {
        conv.ask('You did not select any item');
    }
});


// Set the DialogflowApp object to handle the HTTPS POST request.
exports.dialogflowFirebaseFulfillment = functions.https.onRequest(app);

admin.initializeApp(functions.config().firebase);
var db = admin.database();

const carpark_collection_name = 'carparks';
const carpark_location_collection_name = 'carpark_locations';

const traffic_collection_name = 'traffic_images';
const traffic_location_collection_name = 'traffic_image_locations';

// Create a GeoFire reference
var carparkCollection = db.ref(carpark_collection_name);
var carparkLocationCollection = new GeoFire(db.ref(carpark_location_collection_name));

var trafficCollection = db.ref(traffic_collection_name);
var trafficLocationCollection = new GeoFire(db.ref(traffic_location_collection_name));

exports.loadCarparkInfo = functions.https.onRequest((req, res) => {
    if (!req.body.carparks || req.body.carparks.length <= 0) {
        res.status(400).send({ error: 'Car park information cannot be empty' });
    } else {
        let count = 0;
        let done = (status, ref) => {
            count++;
            if (count >= req.body.carparks.length) {
                res.status(201).send({ message: 'No of records added ' + req.body.carparks.length, count: count });
            }
        };
        for (let item of req.body.carparks) {
            setInFirebase(item, 'car_park_no', carpark_collection_name, carparkLocationCollection, done);
        }
    }
});

exports.fetchCarparkAvailability = functions.https.onRequest((req, res) => {
    if (!req.body.carpark_data || req.body.carpark_data.length <= 0) {
        res.status(400).send({ error: 'Car park information cannot be empty' });
    } else {
        multiUpdateAvailability(req.body.carpark_data, (err, status) => {
            if (err) res.status(503).send({ message: 'Error while updating documents', error: err });
            else res.status(200).send({ message: 'No of records updated ' + req.body.carpark_data.length, status: status });
        })
    }
});


exports.fetchAvailableTrafficImages = functions.https.onRequest((req, res) => {
    if (!req.body.traffic_images || req.body.traffic_images.length <= 0) {
        res.status(400).send({ error: 'traffic image information cannot be empty' });
    } else {
        let count = 0;
        let done = (status, ref) => {
            count++;
            if (count >= req.body.traffic_images.length) {
                res.status(201).send({ message: 'No of records added ' + req.body.traffic_images.length, count: count });
            }
        };
        for (let item of req.body.traffic_images) {
            setInFirebase(item, 'camera_id', traffic_collection_name, trafficLocationCollection, done);
        }
    }
});

exports.fetchNearByCarParks = functions.https.onRequest((req, res) => {
    let latitude = req.query.latitude;
    let longitude = req.query.longitude;
    if (!latitude || !longitude) {
        res.status(400).send({ error: 'latitude & longitude are mandatory query parameters' })
    } else {
        queryFirebaseByLocation(latitude, longitude, req.query.radius, req.query.limit, req.query.skip,
            carpark_collection_name, carparkLocationCollection, 'carpark_info')
            .then(data => res.status(200).send(data))
            .catch(err => res.status(503).send({ error: 'Error while fetching documents', msg: err }));
    }
});

exports.fetchNearBytrafficImages = functions.https.onRequest((req, res) => {
    let latitude = req.query.latitude;
    let longitude = req.query.longitude;
    if (!latitude || !longitude) {
        res.status(400).send({ error: 'latitude & longitude are mandatory query parameters' })
    } else {
        queryFirebaseByLocation(latitude, longitude, req.query.radius, req.query.limit, req.query.skip,
            traffic_collection_name, trafficLocationCollection, 'image')
            .then(data => res.status(200).send(data))
            .catch(err => res.status(503).send({ error: 'Error while fetching documents', msg: err }));
    }
});

function displayCarparkResults(conv, radiusInMeters, limit, skip) {
    return queryFirebaseByLocation(conv.data.latitude, conv.data.longitude, radiusInMeters, limit, skip,
        carpark_collection_name, carparkLocationCollection, 'carpark_info').then(results => {
            if (results.data.length === 0) {
                conv.ask(`Sorry, I couldn't find any car park within ${conv.data.radius.amount} ${conv.data.radius.unit}. Please try with a larger radius`);
            } else {
                if (conv.surface.capabilities.has('actions.capability.SCREEN_OUTPUT') && conv.surface.capabilities.has('actions.capability.WEB_BROWSER')) {
                    let nextSetOfStr = skip > 0 ? 'next set of ' : '';
                    let origin = { latitude: conv.data.latitude, longitude: conv.data.longitude };
                    conv.ask(`<speak><p><s>Here are the ${nextSetOfStr}${results.data.length} nearest car parks out of ${results.total} found near by you.</s></p></speak>`);
                    conv.ask(toCarparkAsBrowseCarousel(results.data, origin));
                } else {
                    let lastUpdateAt = moment.tz(results.data[0].update_datetime, 'Asia/Singapore').fromNow();
                    let distance = results.data[0].distance;
                    let distanceFromUser = (distance >= 1000) ? (Number(convert(distance).from('m').to('km')).toFixed(2) + ' km') : (Number(distance).toFixed() + ' m');
                    let nextStr = skip > 0 ? 'next ' : '';
                    const nearestRecord = `<speak>
                <p><s>Found ${results.total} car parks near to your location. <break time="1"/></s></p>
                <p>
                    <s>The ${nextStr}nearest one is a ${results.data[0].type_of_parking_system} and is located at ${results.data[0].address} in <say-as interpret-as="unit">${distanceFromUser}</say-as> from your location.</s>
                    <s>This car park is a ${results.data[0].car_park_type} and currently has ${results.data[0].carpark_info[0].lots_available} available lots out of ${results.data[0].carpark_info[0].total_lots} lots.</s>
                    <s><break time="1"/>This information is last updated at ${lastUpdateAt}</s>
                </p>
                </speak>`;
                    conv.ask(nearestRecord);
                }
                if (results.nextSkip) {
                    let context = conv.contexts.get('load_more_context');
                    conv.contexts.set('load_more_context', context.lifespan, { limit: limit, nextSkip: results.nextSkip })
                    conv.ask(new Suggestions(['Load more', 'Show next']));
                } else {
                    conv.ask(new Suggestions(['Find car parks', 'Show traffic images', 'Find car parks in 500m', 'Find car parks in 5km', 'Traffic images in 5km']));
                    conv.contexts.delete('load_more_context');
                }
            }
            return Promise.resolve();
        }).catch(err => {
            console.error('Error while fetching near by carparks', err);
            if (err) conv.close("I'm sorry, but something went wrong. Please try again later.");
            return Promise.resolve();
        });
}

function displayTrafficImageResults(conv, radiusInMeters, limit, skip) {
    return queryFirebaseByLocation(conv.data.latitude, conv.data.longitude, radiusInMeters, limit, skip,
        traffic_collection_name, trafficLocationCollection, 'image').then(results => {
            if (results.data.length === 0) {
                conv.ask(`Sorry, I couldn't find any traffic images within ${conv.data.radius.amount} ${conv.data.radius.unit}. Please try with a larger radius`);
            } else {
                if (conv.surface.capabilities.has('actions.capability.SCREEN_OUTPUT')) {
                    let nextSetOfStr = skip > 0 ? 'next set of ' : '';
                    let origin = { latitude: conv.data.latitude, longitude: conv.data.longitude };
                    conv.ask(`<speak><p><s>Here are the ${nextSetOfStr}${results.data.length} nearest traffic images out of ${results.total} found near by you.</s></p></speak>`);
                    conv.ask(toTrafficImagesAsCarousel(results.data, origin));
                } else {
                    conv.ask('Sorry, you device is not capable of displaying images');
                }
                if (results.nextSkip) {
                    let context = conv.contexts.get('load_more_context');
                    conv.contexts.set('load_more_context', context.lifespan, { limit: limit, nextSkip: results.nextSkip })
                    conv.ask(new Suggestions(['Load more', 'Show next']));
                } else {
                    conv.ask(new Suggestions(['Find car parks', 'Show traffic images', 'Find car parks in 500m', 'Find car parks in 5km', 'Traffic images in 5km']));
                    conv.contexts.delete('load_more_context');
                }
            }
            return Promise.resolve();
        }).catch(err => {
            console.error('Error while fetching near by traffic images', err);
            if (err) conv.close("I'm sorry, but something went wrong. Please try again later.");
            return Promise.resolve();
        });
}

function multiUpdateAvailability(carparkData, done) {
    let multiUpdate = {};
    for (let item of carparkData) {
        let key = item.carpark_number;
        multiUpdate[key + '/carpark_info'] = item.carpark_info;
        multiUpdate[key + '/update_datetime'] = item.update_datetime;
    }

    carparkCollection.update(multiUpdate).
        then((status) => {
            console.log('Number of updated documents ', carparkData.length);
            return done(null, status);
        })
        .catch((err) => {
            console.error('Error getting documents', err);
            return done(err);
        });
}

function setInFirebase(item, keyName, collectionName, locationCollection, done) {
    db.ref(collectionName + '/' + item[keyName]).set(item, (error) => {
        if (error) {
            console.error('Error while inserting document', error);
            return done(error);
        } else {
            return locationCollection.set(item[keyName], [Number(item.latitude), Number(item.longitude)])
                .then((ref) => {
                    return done(null, ref);
                }).catch(err => {
                    console.error('Error while inserting location', err);
                    return done(err);
                });
        }
    });
}

function queryFirebaseByLocation(latitude, longitude, radiusInMeters, limit, skip, collectionName, locationCollection, fieldToCheck) {
    return new Promise((resolve, reject) => {
        radiusInMeters = radiusInMeters || 1000;
        limit = limit || 10;
        skip = skip || 0;
        let center = [Number(latitude), Number(longitude)];

        let geoQuery = locationCollection.query({
            center: center,
            radius: Number(radiusInMeters) / 1000
        });
        let results = [];
        let noOfRecords = 0;
        let allReceived = false;
        let receivedSoFar = 0;

        let oneReceived = () => {
            if (allReceived && noOfRecords === receivedSoFar) {
                let limitedResult = getArrayFragment(results.sort(distnaceComparator), parseInt(limit), parseInt(skip));
                resolve(limitedResult);
            }
        }

        let onKeyEnteredRegistration = geoQuery.on('key_entered', (key, location, distance) => {
            noOfRecords++;
            db.ref(collectionName + '/' + key)
                .once('value')
                .then((snapshot) => {
                    let item = snapshot.val();
                    if (item[fieldToCheck]) {
                        item['distance'] = distance * 1000;
                        results.push(item);
                    }
                    receivedSoFar++;
                    return oneReceived();
                }).catch(err => {
                    console.error('Error getting document', err);
                    receivedSoFar++;
                    return oneReceived();
                });
        });

        geoQuery.on("ready", () => {
            // This will fire once the initial data is loaded, so now we can cancel the "key_entered" event listener
            onKeyEnteredRegistration.cancel();
            allReceived = true;
            return oneReceived();
        });
    });
}

function toCarparkAsBrowseCarousel(carparks, origin) {
    let browseCarouselItems = [];
    if (carparks.length === 1) {
        let carpark = carparks[0];
        let mapUrl = `https://www.google.com/maps/dir/?api=1&origin=${origin.latitude},${origin.longitude}&destination=${carpark.latitude},${carpark.longitude}&travelmode=driving`;
        let distanceFromUser = (carpark.distance >= 1000) ? (Number(convert(carpark.distance).from('m').to('km')).toFixed(2) + ' km') : (Number(carpark.distance).toFixed() + ' m');
        let description = `Distance : ${distanceFromUser}. Type: ${carpark.car_park_type} with ${carpark.type_of_parking_system} system. Available lots: ${carpark.carpark_info[0].lots_available}. Total lots: ${carpark.carpark_info[0].total_lots}.`;
        let lastUpdateAt = moment.tz(carpark.update_datetime, 'Asia/Singapore').fromNow();
        return new BasicCard({
            text: `Updated ${lastUpdateAt}`,
            subtitle: description,
            title: carpark.address,
            buttons: new Button({
                title: 'Directions',
                url: mapUrl,
            })
        })
    }

    for (let carpark of carparks) {
        browseCarouselItems.push(toCarparkAsBrowseCarouselItem(carpark, origin));
    }
    return new BrowseCarousel({
        items: browseCarouselItems
    });
}

function toCarparkAsBrowseCarouselItem(carpark, origin) {
    let mapUrl = `https://www.google.com/maps/dir/?api=1&origin=${origin.latitude},${origin.longitude}&destination=${carpark.latitude},${carpark.longitude}&travelmode=driving`;
    let distanceFromUser = (carpark.distance >= 1000) ? (Number(convert(carpark.distance).from('m').to('km')).toFixed(2) + ' km') : (Number(carpark.distance).toFixed() + ' m');
    let description = `Distance : ${distanceFromUser}. Type: ${carpark.car_park_type} with ${carpark.type_of_parking_system} system. Available lots: ${carpark.carpark_info[0].lots_available}. Total lots: ${carpark.carpark_info[0].total_lots}.`;
    let lastUpdateAt = moment.tz(carpark.update_datetime, 'Asia/Singapore').fromNow();
    return new BrowseCarouselItem({
        title: carpark.address,
        url: encodeURI(mapUrl),
        description: description,
        footer: `Updated ${lastUpdateAt}`
    });
}

function toTrafficImagesAsCarousel(trafficImages) {
    let carouselItems = [];
    if (trafficImages.length === 1) {
        let trafficImage = trafficImages[0];
        return toTrafficImageAsBasicCard(trafficImage);
    }

    for (let trafficImage of trafficImages) {
        carouselItems.push(toTrafficImageAsCarouselItem(trafficImage));
    }
    return new Carousel({
        items: carouselItems
    });
}

function toTrafficImageAsCarouselItem(trafficImage) {
    let distanceFromUser = (trafficImage.distance >= 1000) ? (Number(convert(trafficImage.distance).from('m').to('km')).toFixed(2) + ' km') : (Number(trafficImage.distance).toFixed() + ' m');
    let lastUpdateAt = moment.tz(trafficImage.timestamp, 'Asia/Singapore').fromNow();
    return {
        optionInfo: {
            key: trafficImage.camera_id,
            synonyms: [
                trafficImage.display_name,
                trafficImage.camera_id
            ]
        },
        title: trafficImage.display_name,
        description: `Distance ${distanceFromUser}. Updated ${lastUpdateAt}`,
        image: new Image({
            url: trafficImage.image,
            alt: trafficImage.camera_id,
        })
    };
}

function toTrafficImageAsBasicCard(trafficImage) {
    let distanceFromUser = (trafficImage.distance >= 1000) ? (Number(convert(trafficImage.distance).from('m').to('km')).toFixed(2) + ' km') : (Number(trafficImage.distance).toFixed() + ' m');
    let lastUpdateAt = moment.tz(trafficImage.timestamp, 'Asia/Singapore').fromNow();
    return new BasicCard({
        text: `Updated ${lastUpdateAt}`,
        subtitle: `Distance : ${distanceFromUser}`,
        title: trafficImage.display_name,
        image: new Image({
            url: trafficImage.image,
            alt: trafficImage.camera_id,
        })
    });
}

function distnaceComparator(a, b) {
    if (a.distance < b.distance)
        return -1;
    if (a.distance > b.distance)
        return 1;
    return 0;
}

function getArrayFragment(items, limit = 10, skip = 0) {
    return {
        data: items.slice(skip, skip + limit),
        total: items.length,
        nextSkip: skip +
            limit < items.length ? skip + limit : undefined
    }
}