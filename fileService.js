var fs = require('fs');
var co = require('co');

// Used to store entities count
var entitiesInTableCounts = {};
var filesStoredForTableCount = {};
var timeStamps = {};
var partRowEtagLast = {};

var timeStampsOrg = {};
var partRowEtagLastOrg = {};

function init(path) {
    if (path.charAt(path.length - 1) === "/") {
        path = path.substr(0, path.length - 1);
    }
    return new Promise(resolve => {
        function readFile(tableName) {
            return new Promise(resolve => {
                fs.readFile(path + tableName + "/info.json", (err, data) => {
                    if (err) {
                        console.error(err);
                        return resolve(null);
                    }
                    return resolve({ name: tableName, data: JSON.parse(data) });
                });
            });
        }
        function readStoredCountsAndStamps(files) {
            if (!(files && files.length)) {
                return resolve({ timeStamps, entitiesInTableCounts });
            }
            co(function* () {
                var promises = [];
                files.forEach(d => {
                    if (d.indexOf(".") === 0) return;
                    promises.push(readFile(d));
                });
                var infoArray = yield Promise.all(promises);
                infoArray.forEach(i => {
                    if (i && i.data) {
                        timeStamps[i.name] = i.data.timeStamp;
                        timeStampsOrg[i.name] = i.data.timeStamp;
                        entitiesInTableCounts[i.name] = i.data.count;
                        filesStoredForTableCount[i.name] = i.data.fileCount;
                        partRowEtagLast[i.name] = [];
                        partRowEtagLastOrg[i.name] = i.data.partRow;
                    }
                });
                return resolve({ timeStamps, entitiesInTableCounts });
            }).catch(console.error.bind(console));
        }
        function getCountsAndTimeStamps() {
            path = path + "/";
            fs.readdir(path, (err, files) => {
                if (err) {
                    return resolve({ err });
                }
                readStoredCountsAndStamps(files);
            });
        }
        fs.access(path, fs.F_OK, (err) => {
            if (err) {
                return fs.mkdir(path, (err) => {
                    if (err) {
                        return resolve({ err });
                    }
                    // We made a new directory, so no timestamps or counts is stored;
                    resolve({ timeStamps, entitiesInTableCounts });
                });
            }
            getCountsAndTimeStamps();
        });
    });
}
function storeLastTimeStamps(path) {
    return new Promise(resolve => {
        var tableNames = Object.keys(timeStamps);
        var filesToSave = tableNames.length;
        function finished() {
            filesToSave--;
            if (filesToSave <= 0) {
                if (filesToSave < 0) {
                    console.error("storeLastTimeStamps, filesToSave less than 0: " + filesToSave);
                }
                return resolve();
            }
        }
        tableNames.forEach(name => {
            fs.writeFile(path + name + "/info.json", JSON.stringify({ timeStamp: timeStamps[name], fileCount: filesStoredForTableCount[name], count: entitiesInTableCounts[name], partRow: partRowEtagLast[name] }), finished);
        });
    });
}
function saveJson(path, tableName, entities, cb) {
    if (!(entities && entities.length)) {
        return cb();
    }
    var timeStamp = timeStamps[tableName] || 0;
    // Get the last timestamp;
    var toStore = [];
    entities.forEach(e => {
        var time = Date.parse(e.Timestamp._);
        if (time > timeStamp) {
            timeStamp = time;
            partRowEtagLast[tableName] = [];
        }
        var key = e.PartitionKey._ + e.RowKey._ + e[".metadata"].etag;
        var oldTimeStamp = timeStampsOrg[tableName] || 0;
        // Store key for entiti stored with last timeStamp for next backup
        if (time === timeStamp) {
            partRowEtagLast[tableName].push(key);
        }
        // Don't store if already stored.
        if (time === oldTimeStamp && partRowEtagLastOrg[tableName].indexOf(key) > -1) {
            return;
        }
        toStore.push(e);
    });
    timeStamps[tableName] = timeStamp;
    if (!toStore.length) {
        return cb();
    }
    var count = entitiesInTableCounts[tableName] || 0;
    var newCount = count + toStore.length;
    entitiesInTableCounts[tableName] = newCount;
    var fileCount = filesStoredForTableCount[tableName] || 0;
    // We begin with 1
    fileCount++;
    filesStoredForTableCount[tableName] = fileCount;
    // In case for some reason _Restore_File_Index_ is in the table name, we use lastIndexOf when searching
    // for the file index.
    var paddedFileCount = ("0000000000" + fileCount).slice(-10);
    var fileName = tableName + "_" + paddedFileCount + "_" + count + "_" + (newCount - 1) + ".json";
    function saveFile() {
        fs.writeFile(path + tableName + "/" + fileName, JSON.stringify(toStore), err => {
            console.log("Saved " + fileName);
            cb(err)
        });
    }
    fs.access(path + tableName, fs.F_OK, (err) => {
        if (err) {
            return fs.mkdir(path + tableName, (err) => {
                if (err) {
                    return cb(err);
                }
                saveFile();
            });
        }
        saveFile();
    });
}

module.exports = { init, saveJson, storeLastTimeStamps };