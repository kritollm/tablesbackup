// Store all entities in all tables in account.
// Does not handle binary. To add support for binary, just check for edm.binary and
// convert to base64. (The best would be to patch the azure table-storage so you don't have to decode and encode.)

var co = require('co');
var tableService = require('./tableService');
var fileService = require('./fileService');
var { account, accountKey, maxTablesSimultaneous, maxEntitiesInFile, savePath } = require("./creds.json");

// We don't wait for files to be saved, so when finsihed we only exit if all
// entities is retrived and there is no files left to save.
var allEntitiesRetrived = false;
var filesLeftToSave = 0;

function exitIfFinsihed() {
    // < 0 should never happen, so log it if it does.
    if (allEntitiesRetrived && filesLeftToSave <= 0) {
        if (filesLeftToSave < 0) {
            console.error("filesLeftToSave is less than 0: " + filesLeftToSave);
        }
        // Store last time stamps, then a small timeOut before exit the process.
        fileService.storeLastTimeStamps(savePath).then(() => {
            console.log("Finished backing up your tables for the account: " + account);
            setTimeout(() => process.exit(0), 200)
        });
    }
}
function saveEntitiesForTable(tableName, entities) {
    filesLeftToSave++;
    fileService.saveJson(savePath, tableName, entities, (err) => {
        filesLeftToSave--;
        if (err) {
            console.error(err);
        }
        exitIfFinsihed();
    });
}
function backUpAllTables(tablesList, maxAsync) {
    //return new Promise(resolve => {
    maxAsync = maxAsync || 10;
    if (!(tablesList && tablesList.length)) {
        return console.log("No tables to back up");
    }
    console.log("Result length: " + tablesList.length);
    var maxAsyncList = [];
    while (tablesList.length) {
        maxAsyncList.push(tablesList.splice(-maxAsync));
    }
    // The first list may be less than maxTablesSimultaneous
    maxAsyncList.reverse();
    return co(function* () {
        // use for loop because of the yield
        var i, l = maxAsyncList.length
        for (i = 0; i < l; i++) {
            var promises = [];
            var tables = maxAsyncList[i];
            tables.forEach(name => {
                promises.push(tableService.getEntitiesInTable(name, maxEntitiesInFile, entities => saveEntitiesForTable(name, entities)));
            });
            // Execute maxTablesSimultaneous number of tables in parallel
            var res = yield Promise.all(promises);
        }
    }).catch(console.error.bind(console));
}

co(function* () {
    var timeStampsAndCounts = yield fileService.init(savePath);
    tableService.init(account, accountKey, timeStampsAndCounts);
    var allTables = yield tableService.getAllTables();
    allTables = allTables; //.filter(t => (t.indexOf("bf") !== 0 || t.indexOf("EventsList") > -1));
    var finished = yield backUpAllTables(allTables, maxTablesSimultaneous);
    allEntitiesRetrived = true;
    exitIfFinsihed();
}).catch(console.error.bind(console));
