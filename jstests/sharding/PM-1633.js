/**
 * @file
 * Test performance of chunk migrations under load
 * <br>
 *
 * ### *Test*
 *
 *   Move one chunk, containing 25k documents, from one shard to another.
 *
 *   While one chunk migrating, benchRun is used to apply a mixed load onto the documents in the
 *   non-migrating chunks.
 *
 *   Throughput of the background load is also reported in metrics starting with "bg_". The goal
 *   of this test is basically for the migration speed to be as high as possible, while impacting
 *   the background load as little as possible.
 *
 *   Unless user specifies otherwise, uses default value for
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/index.html#chunk-migration-and-replication|_secondaryThrottle}
 *   (false for WiredTiger and true for MMAPv1), and
 *   {@link https://docs.mongodb.com/manual/tutorial/manage-sharded-cluster-balancer/#wait-for-delete|_waitForDelete}
 *   is set to true.
 *
 *   The test takes an optional 'thread_levels' parameters: an array specifying the number of threads to use for load.
 *
 *   Uses
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/#chunk-migration-procedure|moveChunk}
 *   command
 *
 *   Results are reported as docs moved / sec.
 *
 * ### *Setup*
 *
 *   Requires a sharded cluster.
 *
 *   Test inserts 100k documents, split evenly into one more chunk than there are shards. One chunk each is permanently moved to each of the
 *   shards. The last chunk is the one the test moves around. (Note that the test maxes out at five moves to avoid a large number of moves
 *   if the cluster has many shards.)
 *
 *   Field a is indexed, containing uniformly random numbers.
 *
 * ### *Notes*
 *
 *   The load that is here used only as background load was subsequently extracted into its own
 *   test in {@link module:workloads/mix}.
 *
 * @module workloads/move_chunk_back_and_forth_load_other_shards
 */

load("jstests/libs/parallelTester.js");

/* global db sharded sh printjson assert reportThroughput sleep */

var default_val = "default";

/**
 * The value for the _secondaryThrottle parameter to the moveChunk command.
 */
var secondary_throttle = secondary_throttle === undefined ? default_val : secondary_throttle;

/**
 * The value for the _waitForDelete parameter to the moveChunk command.
 */
var wait_for_delete = wait_for_delete === undefined ? default_val : wait_for_delete;


/**
 * The number of threads to run in parallel. The default is [0, 4, 64, 128, 256].
 * **Note:** Thread count should be divisible by 4.
 */
var thread_levels = [256];

(function () {

    let st = new ShardingTest({mongos: 1, shards: {rs0: {nodes: 1}, rs1: {nodes: 1}, rs2: {nodes: 1}, rs3: {nodes: 1}, rs4: {nodes: 1}}});

    const dbName = "test";
    const collName = "foo";
    const ns = dbName + '.' + collName; 
    let db = st.s0.getDB(dbName);
    let config = st.s0.getDB("config");

    const shards = config.shards.find().toArray();
    const numShards = shards.length;
    let shard_ids = [];
    shards.forEach(function (shard) {
        shard_ids.push(shard._id);
    });

    Random.setRandomSeed(0);

    const chunkSize = numShards < 5 ? 25000 : 1000; // 25K docs per shard, 1K if the number of shards is large
    const numberDoc = chunkSize * (numShards + 1);  // one chunk remains on each shard and one extra is moved around

    let longStr = "";
    for (let i = 0; i < 100; i++) {
        longStr += "a";
    }

    assert.commandWorked(db.adminCommand({
        enableSharding: db.toString()
    }));

    st.ensurePrimaryShard(dbName, shard_ids[0]);

    function initColl(coll) {
        st.s.adminCommand({shardCollection: coll.getFullName(), key: {id: 1}});

        // Create n + 1 chunks (n = number of shards)
        for (let i = 1; i <= numShards; i++) {
            st.splitAt(coll.getFullName(), {
                id: chunkSize * i
            });
        }

        // Desired distribution:
        // 1 chunk on shard 0
        // 1 chunks on shard 1
        // 1 chunk on shards 2 through N
        for (let i = 0; i < numShards - 1; i++) {
            idToMove = (chunkSize * (i + 2));
            assert.commandWorked(db.adminCommand({
                moveChunk: coll.getFullName(),
                find: { id: idToMove },
                to: shard_ids[i + 1]
            }));
        }

        // Insert a lot of documents evenly into the chunks.
        let bulk = coll.initializeUnorderedBulkOp();
        for (let i = 0; i < numberDoc; i++) {
            bulk.insert({
                id: i,
                a: Random.randInt(1000000), // indexed
                c: longStr // not indexed
            });
        }
        bulk.execute();

        // Index a non-id key.
        coll.ensureIndex({
            a: 1
        });
    }

    // Move the chunk with id: 0 from its shard to the destination shard.
    function moveChunkTo(ns, toShard) {
        var d1 = Date.now();
        moveChunkCommand = {
            moveChunk: ns,
            find: { id: 0},
            to: toShard,
            _waitForDelete: true
        };
        // Use default secondaryThrottle unless it was explicitly specified in the config.
        if (secondary_throttle !== default_val) {
            moveChunkCommand["_secondaryThrottle"] = secondary_throttle;
        }

        var res = db.adminCommand(moveChunkCommand);
        var d2 = Date.now();
        print("moveChunk result: " + tojson(res));
        assert(res.ok);
        return d2 - d1;
    }

    // Creates background load with BenchRun while the moveChunk is happening.
    function startLoad(ns, nThreads) {
        // We don't want to insert or remove from the chunk we're moving since that will change the
        // number of documents to move. We don't want to update the chunk we're moving either
        // because that makes the catchup phase never finish.
        var minWriteId = chunkSize * 3;
        jsTestLog("minWriteID is: " + minWriteId);
        var benchRuns = [];

        var batchSize = 250; // number of documents per vectored insert
        var docSize = 100; // Document size + _id field

        // Make a document of slightly more than the given size.
        function makeDocument(docSize) {
            for (var i = 0; i < docSize; i++) {
                var doc = { id: minWriteId + Random.randInt(numberDoc - minWriteId),
                            a: Random.randInt(1000000),
                            c: "" };
                while(Object.bsonsize(doc) < docSize) {
                    doc.c += "x";
                }
                return doc;
            }
        }
        
        // Make the document array to insert
        var docs = [];
        for (var i = 0; i < batchSize; i++) {
            docs.push(makeDocument(docSize));
        }

        var ops = [{ op: "insert",
                      writeCmd: true,
                      ns: ns,
                      doc: docs },
                    { op: "remove",
                      writeCmd: true,
                      multi: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ minWriteId, numberDoc ]}}},
                    { op: "update",
                      writeCmd: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ minWriteId, numberDoc ]}},
                      update: { $inc: { a: 1 },
                                $set: { c: { "#RAND_STRING": [ { "#RAND_INT": [ 1, 100 ]}]}}}},
                    { op: "findOne",
                      writeCmd: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ minWriteId, numberDoc ]}}}];
        
        ops.forEach(function(op) {
            benchRuns.push(benchStart({ "ops": [op], "parallel": nThreads, "host": st.s0.host }));    
        });
        return benchRuns;
    }

    // Calls serverStatus with the appropriate arguments
    function getServerStatus(db) {
        return db.serverStatus({ rangeDeleter: 1,
                                 metrics: 0,
                                 tcmalloc: 0,
                                 sharding: 0,
                                 network: 0,
                                 connections: 0,
                                 asserts: 0,
                                 extra_info: 0 });
    }

    // Collects the serverStatus every 2 seconds until the provided CountDownLatch hits 0.
    function collectServerStatus(stopCounter) {
        var output = [];

        while (stopCounter.getCount() > 0) {
            output.push(db.serverStatus({ rangeDeleter: 1,
                                 metrics: 0,
                                 tcmalloc: 0,
                                 sharding: 0,
                                 network: 0,
                                 connections: 0,
                                 asserts: 0,
                                 extra_info: 0 }));
            sleep(2000);
        }
        return output;
    }

    function getTestName() {
        return "moveChunkWithLoad_secondaryThrottle_" + secondary_throttle;
    }

    // Takes 2 server status responses and makes an object with relevant throughputs between them.
    function getStatusDiffThroughput(status1, status2){
        var secDiff = (status2.localTime - status1.localTime) / 1000;
        var throughput = {
            time: status2.localTime,
            insert: (status2.opcounters.insert - status1.opcounters.insert) / secDiff,
            query: (status2.opcounters.query - status1.opcounters.query) / secDiff,
            update: (status2.opcounters.update - status1.opcounters.update) / secDiff,
            delete: (status2.opcounters.delete - status1.opcounters.delete) / secDiff,
            command: (status2.opcounters.command - status1.opcounters.command) / secDiff
        };
        return throughput;
    }

    /**
     * Converts results of background load serverStatus monitoring into two forms.
     * It creates a list of the throughputs at each time interval and then puts those into
     * a map from operation type to a list of throughputs.
     */
    function convertStatusResultsToThroughput(results) {
        var throughputs = [];
        var timeline = { time: [], insert: [], query: [], update: [], delete: [], command: [] };
        for(var i = 1; i < results.length; i++) {
            var throughput =  getStatusDiffThroughput(results[i-1], results[i]);
            throughputs.push(throughput);
            timeline.time.push(throughput.time);
            timeline.insert.push(throughput.insert);
            timeline.query.push(throughput.query);
            timeline.update.push(throughput.update);
            timeline.delete.push(throughput.delete);
            timeline.command.push(throughput.command);
        }
        // TODO:  This log could be useful, but the parser currently does not handle it properly.
        // When the parse is fixed, uncomment this line.
        // print("Throughputs: " + tojsononeline(throughputs));
        print("Timeline: " + tojson(timeline));
        return timeline;
    }
    
    function findMedian(values, description) {
        var median;

        values.sort(function(a,b){return a-b;});
        var half = Math.floor(values.length/2);
        if(values.length % 2) {
            median = values[half];
        } else {
            median = (values[half-1] + values[half]) / 2.0;
        }
        
        print(description + ": " + tojson(values));
        return median;
    }

    function testMoveChunkWithLoad(collName, nThreads) {
        var coll = db[collName];
        initColl(coll);

        st.configRS.awaitReplication();
        st.rs0.awaitReplication();
        st.rs1.awaitReplication();
        st.rs2.awaitReplication();
        st.rs3.awaitReplication();
        st.rs4.awaitReplication();
    
        sleep(500);
        st.printChunks();  // check shard status before test run
        var nLogThreads = nThreads * 4;

        jsTest.log("Test moveChunk with\t " +
            " secondaryThrottle=" +  secondary_throttle +
            " nThreads=" + nLogThreads);

        var moveChunkEndCounter = new CountDownLatch(1);
        var serverStatusThread = new Thread(collectServerStatus, moveChunkEndCounter);
        serverStatusThread.start();

        var benchRunTasks;
        if (nThreads > 0) {
            print("Starting load... " + (new Date()));
            benchRunTasks = startLoad(coll.getFullName(), nThreads);
        }

        // Let load start before collecting baseline.
        sleep(2000);

        // Record baseline throughput with load but no moveChunk.
        var beforeBaseStatus = getServerStatus(db);
        sleep(2000);
        var afterBaseStatus = getServerStatus(db);
        var bgBaseThroughput = getStatusDiffThroughput(beforeBaseStatus, afterBaseStatus);

        var bgInsertThroughputs = [];
        var bgQueryThroughputs = [];
        var throughputs = [];

        maxMoves = 10;
        let toShard = shard_ids[1];
        for(var i = 0; i < maxMoves; i++) {
            // Let the system settle after the moveChunk, logging the background throughput.
            var beforeBetweenStatus = getServerStatus(db);
            var afterBetweenStatus = getServerStatus(db);
            var bgBetweenThroughput = getStatusDiffThroughput(beforeBetweenStatus, 
                                                              afterBetweenStatus);
            jsTest.log("Background Throughput Before Migration: " + tojson(bgBetweenThroughput));

            // Perform the actual moveChunk, recording background throughput before and after.
            var beforeStatus = getServerStatus(db);
            print("Starting moveChunk... " + (new Date()));
            var moveChunkTime = moveChunkTo(coll.getFullName(), toShard);
            var afterStatus = getServerStatus(db);
            print("Ending moveChunk... " + (new Date()));

            var bgThroughput = getStatusDiffThroughput(beforeStatus, afterStatus);
            bgInsertThroughputs.push(bgThroughput.insert);
            bgQueryThroughputs.push(bgThroughput.query);
            throughputs.push(chunkSize * 1000 / moveChunkTime);
        
            jsTest.log("moveChunk to shard " + i%maxMoves + " takes " + moveChunkTime +
                " ms with\t secondaryThrottle=" + secondary_throttle +
                " nThreads=" + nThreads * 4);

            jsTest.log("Background Throughput During Chunk Migration: " + tojson(bgThroughput));

            toShard = (toShard == shard_ids[1]) ? shard_ids[0] : shard_ids[1];
        }
            
        // Record throughput while load is still running but moveChunk is done.
        print("Ending Chunk Migrations... " + (new Date()));
        if (nThreads > 0) {
            print("Ending benchRun... " + (new Date()));
            benchRunTasks.forEach(function(benchRunTask) {
                var benchResults = benchFinish(benchRunTask);
                print("BenchRun Results:  " + tojson(benchResults));
            });
        }

        // Record throughput when load is finished and moveChunk is done.
        moveChunkEndCounter.countDown();
        serverStatusThread.join();
        var serverStatusData = serverStatusThread.returnData();
        convertStatusResultsToThroughput(serverStatusData);

        jsTestLog("moveChunk throughputs: " + findMedian(throughputs, "moveChunk throughputs"));
        jsTestLog("baseline insert: " + bgBaseThroughput.insert);
        jsTestLog("baseline query: " + bgBaseThroughput.query);
        jsTestLog("insert: " + findMedian(bgInsertThroughputs, "bg insert throughputs"));
        jsTestLog("query: " + findMedian(bgQueryThroughputs, "bg query throughputs"));
        jsTestLog("total inserts: " + bgInsertThroughputs.reduce((a, b) => a + b));
        jsTestLog("total queries: " + bgQueryThroughputs.reduce((a, b) => a + b));


        coll.drop();
    }

    thread_levels.forEach(function(nThreads) {
        var nThreadsNormalized = Math.floor(nThreads / 4);
        testMoveChunkWithLoad("move_chunk_throttle_" + secondary_throttle + " " + nThreads, nThreadsNormalized);
    })

    //testMoveChunkWithLoad("move_chunk_throttle_" + secondary_throttle + " " + 1, 1);
    st.stop();


})();
