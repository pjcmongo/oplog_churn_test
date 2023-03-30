const { MongoClient } = require("mongodb");
const { Binary } = require ('mongodb');
const { BSON } = require ('mongodb')
const { Document } = require ('mongodb')
const zlib = require("zlib");
const _ = require("lodash");
var fs = require('fs');

async function main() {

    // set uri to MongoDB cluster 
    // example: 
    // const uri = "mongodb+srv://<username>:<password>@<cluster>/?retryWrites=true&w=majority"
    const uri = ""

    // set bulkWrite() batch size 
    // batchSize=100 results in total of 200 actual operations 
    const batchSize = 100
    // set number bulkWrite() operations to invoke
    const numBatches = 1

    if (uri.length == 0) throw new Error('MongoDB uri must be set!');
    const client = new MongoClient(uri);
    const testDb = "oplog-test"

    const t1Collection = "full-doc-replace"
    const t1Ns = testDb + "." + t1Collection

    const t2Collection = "set-auth"
    const t2Ns = testDb + "." + t2Collection 

    const t3Collection = "compress-set-auth"
    const t3Ns = testDb + "." + t3Collection

    const t4Collection = "full-doc-replace-compressed"
    const t4Ns = testDb + "." + t4Collection

    try {
        // Connect to the MongoDB cluster
        await client.connect();

        // test1: replace entire user document 
        await runTest1(client, testDb, t1Ns, t1Collection, batchSize, numBatches)
 
        // test2: $set/$unset journey (startDoc.devices[0].session.auth_session)
        await runTest2(client, testDb, t2Ns, t2Collection, batchSize, numBatches)

        // test3: $set/$unset compressed journey (startDoc.devices[0].session.auth_session)
        await runTest3(client, testDb, t3Ns, t3Collection, batchSize, numBatches)

        // test4: replace entire user document with compressed journey (startDoc.devices[0].session.auth_session)
        await runTest4(client, testDb, t4Ns, t4Collection, batchSize, numBatches)

    } finally {
        // Close the connection to the MongoDB cluster
        console.log("close connection")
        await client.close();
    }
}

main().catch(console.error);

async function runTest1(client, testdb, ns, coll, batchSize, numBatches) {
    console.log("\n********* test1: replace entire user document")

    await dropCollection(client, testdb, coll)

    const fs1 = fs.readFileSync('./test1/userWithoutJourney.json','utf8');
    const fs2 = fs.readFileSync('./test1/userWithJourney.json','utf8');
    const initDoc = JSON.parse(fs1)
    const replaceDoc = JSON.parse(fs2)

    delete initDoc._id
    delete replaceDoc._id

    const initDocBsize = BSON.calculateObjectSize(initDoc);
    const replaceDocBsize = BSON.calculateObjectSize(replaceDoc);
    console.log("user doc without journey BSON size: " + initDocBsize);
    console.log("user doc with journey BSON size: " + replaceDocBsize);

    const result = await client.db(testdb).collection(coll).insertOne(initDoc);

    // delay 2 seconds to avoid catching insert in OpLog summary
    await new Promise(resolve => setTimeout(resolve, 2000));

    let ops = []
    for(let n=0 ; n < batchSize ; ++n) {
        ops.push(buildReplace(replaceDoc, result.insertedId))
        ops.push(buildReplace(initDoc, result.insertedId))
    }  
    const startTime = new Date()          
    console.log("test 1 start: " + startTime.toUTCString())
    await runTest(client, testdb, coll, ops, numBatches)
    const stopTime = new Date()
    console.log("\ntest 1 complete: " + stopTime.toUTCString())

    const opLogSummary = await getOplogSummary(client, startTime,stopTime)
    console.log("test1 results:")
    console.log("startTime: " + opLogSummary.startTime.toUTCString())
    console.log("stopTime: " + opLogSummary.endTime.toUTCString())
    console.log("collection: " + ns)
    console.log(opLogSummary[`${ns}`])
}

async function runTest2(client, testdb, ns, coll, batchSize, numBatches) {
    console.log("\n********* test2: $set/$unset journey")

    await dropCollection(client, testdb, coll)

    const fs1 = fs.readFileSync('./test2/userWithJourney.json','utf8');
    const userDoc = JSON.parse(fs1)
    const auth = userDoc.devices[0].session.auth_session

    delete userDoc._id

    const authBsize = BSON.calculateObjectSize(auth);
    console.log("journey BSON size: " + authBsize);

    const result = await client.db(testdb).collection(coll).insertOne(userDoc);

    // delay 2 seconds to avoid catching insert in OpLog summary
    await new Promise(resolve => setTimeout(resolve, 2000));

    let ops = []
    for(let n=0 ; n < batchSize ; ++n) {
        ops.push(buildUnsetAuth(result.insertedId))
        ops.push(buildSetAuth(auth, result.insertedId))
    }  
    const startTime = new Date()          
    console.log("test 2 start: " + startTime.toUTCString())
    await runTest(client, testdb, coll, ops, numBatches)
    const stopTime = new Date()
    console.log("\ntest 2 complete: " + stopTime.toUTCString())

    const opLogSummary = await getOplogSummary(client, startTime,stopTime)
    console.log("test2 results:")
    console.log("startTime: " + opLogSummary.startTime.toUTCString())
    console.log("stopTime: " + opLogSummary.endTime.toUTCString())
    console.log("collection: " + ns)
    console.log(opLogSummary[`${ns}`])

}

async function runTest3(client, testdb, ns, coll, batchSize, numBatches) {
    console.log("\n********* test3: $set/$unset compressed journey")

    await dropCollection(client, testdb, coll)

    const fs1 = fs.readFileSync('./test3/userWithJourney.json','utf8');
    const userDoc = JSON.parse(fs1)
    const auth = userDoc.devices[0].session.auth_session

    delete userDoc._id

    verify(auth)

    const result = await client.db(testdb).collection(coll).insertOne(userDoc);
 
    // delay 2 seconds to avoid catching insert in OpLog summary
    await new Promise(resolve => setTimeout(resolve, 2000));

    let ops = []
    for(let n=0 ; n < batchSize ; ++n) {
        ops.push(buildUnsetAuth(result.insertedId))
        ops.push(buildSetAuthCompress(auth, result.insertedId))
    }  
    const startTime = new Date()          
    console.log("test 3 start: " + startTime.toUTCString())
    await runTest(client, testdb, coll, ops, numBatches)
    const stopTime = new Date()
    console.log("\ntest 3 complete: " + stopTime.toUTCString())

    const opLogSummary = await getOplogSummary(client, startTime,stopTime)
    console.log("test3 results:")
    console.log("startTime: " + opLogSummary.startTime.toUTCString())
    console.log("stopTime: " + opLogSummary.endTime.toUTCString())
    console.log("collection: " + ns)
    console.log(opLogSummary[`${ns}`])

    const retAuth = await getAuthDecompressed(client, testdb, coll, result.insertedId)
    console.log("retrieve/decompress/deserialize journey and check equality:" + _.isEqual(auth,retAuth))
}

async function runTest4(client, testdb, ns, coll, batchSize, numBatches) {
    console.log("\n********* test4: replace user entire user document with compressed journey")

    await dropCollection(client, testdb, coll)

    const fs1 = fs.readFileSync('./test4/userWithJourney.json','utf8');
    let userDoc = JSON.parse(fs1)

    const fs2 = fs.readFileSync('./test4/userWithJourney.json','utf8');
    let userDocWithoutAuth = JSON.parse(fs2)

    delete userDoc._id
    delete userDocWithoutAuth._id

    // prepare user doc (copy) without journey
    delete userDocWithoutAuth.devices[0].session.auth_session

    // prepare user doc with compressed journey
    const auth = userDoc.devices[0].session.auth_session
    const as = JSON.stringify(auth);
    const authCompressed = zlib.deflateSync(as);
    userDoc.devices[0].session.auth_session = new Binary(authCompressed)

    const userWithoutAuthBsonSize = BSON.calculateObjectSize(userDocWithoutAuth);
    console.log("user doc without journey BSON size: " + userWithoutAuthBsonSize);

    const userWithAuthCompBsonSize = BSON.calculateObjectSize(userDoc);
    console.log("user doc with journey compressed BSON size: " + userWithAuthCompBsonSize);

    const result = await client.db(testdb).collection(coll).insertOne(userDoc);

    // delay 2 seconds to avoid catching insert in OpLog summary
    await new Promise(resolve => setTimeout(resolve, 2000));

    let ops = []
    for(let n=0 ; n < batchSize ; ++n) {
        ops.push(buildReplace(userDocWithoutAuth, result.insertedId))
        ops.push(buildReplace(userDoc, result.insertedId))
    }  

    const startTime = new Date()          
    console.log("test 4 start: " + startTime.toUTCString())
    await runTest(client, testdb, coll, ops, numBatches)
    const stopTime = new Date()
    console.log("\ntest 4 complete: " + stopTime.toUTCString())

    const opLogSummary = await getOplogSummary(client, startTime,stopTime)
    console.log("test4 results:")
    console.log("startTime: " + opLogSummary.startTime.toUTCString())
    console.log("stopTime: " + opLogSummary.endTime.toUTCString())
    console.log("collection: " + ns)
    console.log(opLogSummary[`${ns}`])

    const retAuth = await getAuthDecompressed(client, testdb, coll, result.insertedId)
    console.log("retrieve/decompress/deserialize journey and check equality:" + _.isEqual(auth,retAuth))

}

function updateSummary(summary, doc) {
    if (!("startTime" in summary)) summary.startTime = doc.wall
    summary.endTime = doc.wall

    if (!(doc.ns in summary)) {
        summary[`${doc.ns}`] = {}
        if( doc.ns == "") {
            summary[`${doc.ns}`].noop = 0    
        }
        else {
            summary[`${doc.ns}`].update = 0
            summary[`${doc.ns}`].insert = 0
            summary[`${doc.ns}`].command = 0
            summary[`${doc.ns}`].delete = 0
        }
        summary[`${doc.ns}`].maxBsonSize = 0
        summary[`${doc.ns}`].wallForMaxBsonSize = 0
        summary[`${doc.ns}`].stmtIdForMaxBsonSize = 0
        summary[`${doc.ns}`].totalBsonSize = 0
        summary[`${doc.ns}`].avgBsonSize = 0
        summary[`${doc.ns}`].totalOps = 0
    }
    var bsize = BSON.calculateObjectSize(doc);
    if(bsize > summary[`${doc.ns}`].maxBsonSize) {
        summary[`${doc.ns}`].maxBsonSize = bsize
        summary[`${doc.ns}`].wallForMaxBsonSize = doc.wall
        summary[`${doc.ns}`].stmtIdForMaxBsonSize = doc.stmtId
    }
    summary[`${doc.ns}`].totalBsonSize += bsize
    ++summary[`${doc.ns}`].totalOps
    summary[`${doc.ns}`].avgBsonSize = summary[`${doc.ns}`].totalBsonSize / summary[`${doc.ns}`].totalOps

    switch (doc.op) {
        case "c": 
            ++summary[`${doc.ns}`].command;
            break;
        case "u": 
            ++summary[`${doc.ns}`].update;
            break;
        case "i": 
            ++summary[`${doc.ns}`].insert;
            break;
        case "d": 
            ++summary[`${doc.ns}`].delete;
            break;
        case "n":
            ++summary[`${doc.ns}`].noop;
            break;
    }
    return summary;
}

async function getOplogSummary(client, start_date = new Date(new Date().setDate(new Date().getDate()-1)), stop_date = new Date())  {
    let summary = {}
    let docCount = 0
    // add 1 second to stop time to ensure catch all operation
    let stopTimeOffset = new Date(new Date().setSeconds(stop_date.getSeconds()+1))
    let st = new Date()
    console.log("GetOpLogSummary start: " + start_date.toUTCString() + " - " + stopTimeOffset.toUTCString())
    await client.db("local").collection("oplog.rs").find({"wall" : { $gte: start_date, $lt: stopTimeOffset } }).forEach(function(doc) {
        if (docCount && !(docCount % 100000)) {
            console.log(`${docCount} docs processed, wall:` + doc.wall.toISOString() + ` in ${ISODate()-st} ms`)
        }
        ++docCount
        summary = updateSummary(summary, doc)
    });

    console.log(`${docCount} docs processed in ${new Date()-st} milliseconds`)
    return summary;
}

async function dropCollection(client, d, c) {
    try {
        const result = await client.db(d).collection(c).drop();
        console.log(`dropped ${d}.${c} ${result}`);
    } catch(e) {
        console.log(`error dropping ${d}.${c} ${e.message}`);
    }
}

async function runTest(client, d, c, ops, numBatches) {
    process.stdout.write("running");    
    for(let x=0 ; x < numBatches ; ++x) {
        process.stdout.write(".");      
        const result = await client.db(d).collection(c).bulkWrite(ops)
    }
}

function buildReplace(replacement, targetId) {
    //delete doc._id
    var op = { replaceOne: {
            filter: { _id: targetId }, 
            replacement: replacement 
        }
    }
    return op;
}

function buildSetAuth(auth, targetId) {
    var op = { updateOne: {
        filter: { _id: targetId },
        update: { $set: {"devices.0.session.auth_session": auth}}
    }}
    return op;
}

function buildUnsetAuth(targetId) {
    var op = { updateOne: {
        filter: { _id: targetId },
        update: { $unset: {"devices.0.session.auth_session": ""}}
    }}
    return op;
}


function buildSetAuthCompress(auth, targetId) {
    let as = JSON.stringify(auth);
    var asComp = zlib.deflateSync(as);

    var op = { updateOne: {
            filter: { _id: targetId },
            update: { $set: {"devices.0.session.auth_session": new Binary(asComp)}}
        }
    }

    return op;
}

async function getAuthDecompressed(client, d, c, targetId) {
    const result = await client.db(d).collection(c).findOne({_id: targetId});
    var asd = zlib.inflateSync(result.devices[0].session.auth_session.buffer);
    return JSON.parse(asd);
}




async function setAuth(client, auth, targetId) {
    let as = JSON.stringify(auth);
    var asComp = zlib.deflateSync(as);

    const result = await client.db("perf").collection("perftest").updateOne(
            { _id: targetId }, { $set: {"devices.0.session.auth_session": Binary(asComp)} }
        );
 
    //console.log(`${result.matchedCount} document(s) matched the query criteria.`);
    //console.log(`${result.modifiedCount} document(s) was/were updated.`);
}




function verify(auth) {
    let as = JSON.stringify(auth);
    var asComp = zlib.deflateSync(as);

    console.log(`zlib returns Buffer: ${Buffer.isBuffer(asComp)}`);
    console.log(`stringified journey length ${as.length}`)  
    console.log(`compressed journey bytes ${Buffer.byteLength(asComp)}`)

    const authCompBsize = BSON.calculateObjectSize(new Binary(asComp));
    console.log("compressed journey BSON size: " + authCompBsize);

    var asd = zlib.inflateSync(asComp);

    console.log(`uncompressed stringify journey length ${asd.toString().length}`)
    //console.log(buffer.toString('base64'));

    let auth2 = JSON.parse(asd)
    console.log("check equality before/after compress:" + _.isEqual(auth,auth2))
}

async function unsetAuth(client, targetId) {
    const result = await client.db("perf").collection("perftest").updateOne(
            { _id: targetId }, { $unset: {"devices.0.session.auth_session": ""} }
    );
}
    
