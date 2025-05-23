const API_HUB = require('apihub');
let config = API_HUB.getServerConfig();
const {migrateDataToLightDB} = require('./migrationUtils');
const LightDBEnclaveFactory = require("./LightDBEnclaveFactory");
const {getDiffsForAudit} = require("./Events");
const process = require("process");
const openDSU = require("opendsu");
const enclaveAPI = openDSU.loadAPI("enclave");
const crypto = openDSU.loadAPI("crypto");
const constants = require("./constants");
const MIGRATION_STATUS = constants.MIGRATION_STATUS;
const APP_NAME = "Demiurge";
const PREFIX = 'DB_';
const generateEnclaveName = (domain, subdomain) => `${PREFIX}${domain}_${subdomain}_${APP_NAME}`;


const MIGRATION_SECRET_NAME = "demiurge_migration";
let migrationStatus = MIGRATION_STATUS.NOT_STARTED;

const migrationDone = async () => {
    const secretsServiceInstance = await API_HUB.getSecretsServiceInstanceAsync(config.storage);
    let secret;
    try {
        secret = secretsServiceInstance.readSecretFromDefaultContainerSync(MIGRATION_SECRET_NAME);
    } catch (e) {
        console.log("Failed to read secret", MIGRATION_SECRET_NAME, e);
    }
    if (secret && secret === process.env.EPI_VERSION) {
        return true;
    }

    return false;

}
const getDemiurgeSharedEnclave = (keySSI, callback) => {
    const epiEnclave = enclaveAPI.initialiseWalletDBEnclave(keySSI);

    epiEnclave.on("error", (err) => {
        return callback(err);
    })

    epiEnclave.on("initialised", async () => {
        callback(undefined, epiEnclave);
    });
}

const getDemiurgeSharedEnclaveAsync = async (keySSI) => {
    return $$.promisify(getDemiurgeSharedEnclave)(keySSI);
}

const generateAndSaveSlotToSecrets = async (domain, subdomain) => {
    const slot = crypto.generateRandom(32).toString("base64");
    const secretsServiceInstance = await API_HUB.getSecretsServiceInstanceAsync(config.storage);
    await secretsServiceInstance.putSecretInDefaultContainerAsync(generateEnclaveName(domain, subdomain, APP_NAME), slot);
}

const migrateDataFromDemiurgeSharedEnclaveToLightDB = async (domain, subdomain, demiurgeSharedEnclaveKeySSI) => {
    migrationStatus = MIGRATION_STATUS.IN_PROGRESS;
    let demiurgeSharedEnclave;
    try {
        demiurgeSharedEnclave = await getDemiurgeSharedEnclaveAsync(demiurgeSharedEnclaveKeySSI);
    } catch (e) {
        console.error("Failed to get epi enclave", e);
        migrationStatus = MIGRATION_STATUS.FAILED;
        throw e;
    }

    try {
        await generateAndSaveSlotToSecrets(domain, subdomain);
    } catch (e) {
        console.error("Failed to generate slot and save to secrets", e);
        migrationStatus = MIGRATION_STATUS.FAILED;
        throw e;
    }
    console.log("Slot generated and saved to secrets");

    const LightDBEnclaveFactory = require("./LightDBEnclaveFactory");
    const lightDBEnclaveFactory = LightDBEnclaveFactory.getLightDBEnclaveFactoryInstance();
    const lightDBEnclave = await lightDBEnclaveFactory.createLightDBEnclaveAsync(domain, subdomain, {appName: APP_NAME, skipCache: true});

    const transformUserAccess = record => {
        record.username = record.actionUserId;
        record.userGroup = record.group;

        return record;
    }

    const generatePkForAudit = record => {
        let pk = record.pk;
        if (typeof pk !== "string") {
            pk = JSON.stringify(pk);
        }

        if (!pk) {
            pk = openDSU.loadAPI("crypto").generateRandom(32).toString("hex");
        }

        return pk;
    }


    try {
        // Use the generalized migration function for different tables with appropriate transformations
        await migrateDataToLightDB(demiurgeSharedEnclave, lightDBEnclave, "demiurge_logs_table", "audit", transformUserAccess, generatePkForAudit);
        console.log("Audit migrated")
    } catch (e) {
        console.error("Failed to migrate data", e);
        migrationStatus = MIGRATION_STATUS.FAILED;
        throw e;
    }

    try {
        const secretsServiceInstance = await API_HUB.getSecretsServiceInstanceAsync(config.storage);
        console.log(`updating secret ${MIGRATION_SECRET_NAME} with value ${process.env.EPI_VERSION}`)
        await secretsServiceInstance.putSecretInDefaultContainerAsync(MIGRATION_SECRET_NAME, process.env.EPI_VERSION);
        console.log("=============================================================")
        console.log("Audit migration completed");
        console.log("=============================================================")
    } catch (e) {
        console.error("Failed to mark migration as done", e);
        migrationStatus = MIGRATION_STATUS.FAILED;
        throw e;
    }
}

const getDemiurgeMigrationStatus = async () => {
    let migrationIsDone = await migrationDone();
    if (migrationIsDone) {
        return MIGRATION_STATUS.COMPLETED;
    }

    return migrationStatus;
}

module.exports = {
    getDemiurgeMigrationStatus,
    migrateDataFromDemiurgeSharedEnclaveToLightDB
}
