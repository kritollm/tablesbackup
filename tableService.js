var azure = require('azure-storage');
var tableService;
var QueryComparisons = azure.TableUtilities.QueryComparisons;
var TableQuery = azure.TableQuery;
var TableOperators = azure.TableUtilities.TableOperators;

var co = require('co');

var tableCounts = {};
var timeStamps = {};

function init(account, accountKey, timeStampsAndCounts) {
    timeStamps = timeStampsAndCounts.timeStamps;
    tableCounts = timeStampsAndCounts.tableCounts;
    tableService = azure.createTableService(account, accountKey);
}

function listTables(continuationToken, next, finished) {
    var options = { maxResults: 20 };
    tableService.listTablesSegmented(continuationToken, options, (error, result) => {


        var continuationToken = result.continuationToken;
        var entries = result.entries;
        if (error || !(continuationToken)) {
            return finished(error, entries);
        }
        next(entries, continuationToken);
    });
}

function getAllTables() {
    var allTables = [];
    return new Promise(resolve => {
        function finished(error, entries) {
            if (entries) {
                allTables = allTables.concat(entries);
            }
            resolve(allTables);
        }
        function next(entries, continuationToken) {
            if (entries) {
                allTables = allTables.concat(entries);
            }
            listTables(continuationToken, next, finished);
        }
        listTables(null, next, finished);
    });
}

function listTablesCO(continuationToken) {
    var options = {};
    // For testing of the continuationToken
    //options.maxResults = 20;
    return new Promise(resolve => {
        tableService.listTablesSegmented(continuationToken, options, (error, result) => {
            var continuationToken = result && result.continuationToken;
            var entries = result && result.entries;
            resolve({ error, continuationToken, entries });
        });
    });

}
function getAllTablesCO() {
    var allTables = [];
    return co(function* () {
        var finished = false;
        var continuationToken = null;
        while (!finished) {
            var res = yield listTablesCO(continuationToken);
            continuationToken = res.continuationToken;
            if (res.error || !(continuationToken)) {
                finished = true;
            }
            if (res.entries && res.entries.length) {
                allTables = allTables.concat(res.entries);
            }
        }
        return allTables;
    }).catch(console.error.bind(console));
}

function getEntities(tableName, continuationToken, query) {
    return new Promise(resolve => {
        tableService.queryEntities(tableName, query, continuationToken, function (error, result, response) {
            var entries = result && result.entries;
            var continuationToken = result && result.continuationToken
            resolve({ entries, error, continuationToken });
        });
    });
}
// maxCB is a callback to execute when entities reach maxEntities
function getEntitiesInTable(tableName, maxEntities, maxCB) {
    var entities = [];
    var query;
    var timeStamp = timeStamps[tableName];
    if (timeStamp) {
        // Timestamp is stored as number
        var filter = TableQuery.dateFilter('Timestamp', QueryComparisons.GREATER_THAN_OR_EQUAL, new Date(timeStamp));
        query = new TableQuery().where(filter);
    } else {
        query = new TableQuery();
    }
    return co(function* () {
        var finished = false;
        var continuationToken = null;
        while (!finished) {
            var res = yield getEntities(tableName, continuationToken, query);
            continuationToken = res.continuationToken;
            if (res.error || !(continuationToken)) {
                if (res.error) {
                    console.error(res.error);
                }
                finished = true;
            }
            if (res.entries && res.entries.length) {
                entities = entities.concat(res.entries);
            }
            while (entities.length >= maxEntities) {
                maxCB(entities.splice(0, maxEntities));
            }
        }
        maxCB(entities.splice(0, entities.length))
    }).catch(console.error.bind(console));
}
var myService = { getAllTables: getAllTablesCO, init, getEntitiesInTable };

module.exports = myService;
