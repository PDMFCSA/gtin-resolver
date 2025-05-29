const EnclaveFacade = require("loki-enclave-facade");
const apihubModule = require("apihub");
require("opendsu");

const config = apihubModule.getServerConfig();
const DBService = EnclaveFacade.DBService;
let dbService;

// Generalized migration function
const migrateDataToLightDB = async (walletDBEnclave, lightDBEnclave, sourceTableName, targetTableName, transformRecord = async record => await record, generatePK = record => record.pk) => {
    let tables;
    try{
        tables = await $$.promisify(walletDBEnclave.getAllTableNames)($$.SYSTEM_IDENTIFIER);
    }catch (e) {
        console.error("Failed to get tables", e);
    }

    console.log("====================================================================================================");
    console.log(tables);
    console.log("====================================================================================================");

    console.log("====================================================================================================");
    console.log(`Trying to migrate records from table ${sourceTableName} to table ${targetTableName}`);
    console.log("====================================================================================================");

    let records;
    try {
        records = await $$.promisify(walletDBEnclave.getAllRecords)(undefined, sourceTableName);
        console.log(`Preparing to migrate ${records.length} records from table ${sourceTableName} to table ${targetTableName}`);
    } catch (e) {
        console.error("Failed to get records from table", sourceTableName, e);
        throw e;
    }

    const mapRecordToCouchDB = async (record) => {
        delete record.meta;
        delete record.$loki;
        record["timestamp"] = record.__timestamp;
        delete record.__timestamp;
        delete record.__version;
    
        if(!record.timestamp)
            delete record.timestamp;
    
        return record
    }

    let counter = 0;
    for (let record of records) {
        const transformedRecord = await transformRecord(record);
        const couchRecord = await mapRecordToCouchDB(transformedRecord)

        let existingRecord;
        try {
            existingRecord = await $$.promisify(lightDBEnclave.getRecord)($$.SYSTEM_IDENTIFIER, targetTableName, generatePK(record));
        } catch (e) {
            //table does not exist
        }

        if (!existingRecord) {
            try {
                counter++;
                await $$.promisify(lightDBEnclave.insertRecord)($$.SYSTEM_IDENTIFIER, targetTableName, generatePK(record), couchRecord);
            } catch (e) {
                console.error("Failed to insert record", couchRecord, "in table", targetTableName, e);
                throw e;
            }
        }
    }
    try {
        await $$.promisify(lightDBEnclave.saveDatabase)($$.SYSTEM_IDENTIFIER);
    } catch (e) {
        console.error("Failed to save database", e);
        throw e;
    }
    console.log(`Migrated ${counter} records from table ${sourceTableName} to table ${targetTableName}`);
};

const createMetadataFixedUrl = async (walletDBEnclave, sourceTableName, domain, subdomain) => {
    let tables;
    try{
        tables = await $$.promisify(walletDBEnclave.getAllTableNames)($$.SYSTEM_IDENTIFIER);
    }catch (e) {
        console.error("Failed to get tables", e);
    }

    console.log("====================================================================================================");
    console.log(tables);
    console.log("====================================================================================================");

    console.log("====================================================================================================");
    console.log(`Trying to create metadata for records from table ${sourceTableName}`);
    console.log("====================================================================================================");

    let records;

    const userName = process.env.DB_USER || config.db.user;
    const secret = process.env.DB_SECRET || config.db.secret;

    dbService = new DBService( {
        uri: config.db.uri,
        username: userName,
        secret: secret,
        debug: config.db.debug,
        readOnlyMode: process.env.READ_ONLY_MODE || false
    });
    // try {
    //     records = await $$.promisify(walletDBEnclave.getAllRecords)(undefined, sourceTableName);
    //     console.log(`Preparing to create ${records.length} metadata fixed urls records from table ${sourceTableName}`);
    // } catch (e) {
    //     console.error("Failed to get records from table", sourceTableName, e);
    //     throw e;
    // }

    let fixedUrlUtils = require("../../mappings/utils.js");

    for (let record of records) { 
        try {
            await fixedUrlUtils.registerLeafletMetadataFixedUrlByDomainAsync(domain, subdomain, record.productCode, record.batchNumber || undefined);  
            await fixedUrlUtils.deactivateMetadataFixedUrl(undefined, "metadata", domain, record.productCode, record.batchNumber, undefined, undefined, undefined);
            await fixedUrlUtils.activateMetadataFixedUrl(undefined, "metadata", domain, record.productCode, record.batchNumber, undefined, undefined, undefined);
        } catch (e) {
            console.error("Failed to create metadata fixed url: ", record, "/n",  e);
        }
    }
    
    console.log("Completed creating metadata fixed urls for domain ", domain, "subdomain ", subdomain, "for all ", sourceTableName);
}

module.exports = {
    migrateDataToLightDB,
    createMetadataFixedUrl
}