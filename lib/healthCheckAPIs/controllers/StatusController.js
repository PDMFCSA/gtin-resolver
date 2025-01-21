const openDSU = require('opendsu');
const keySSISpace = openDSU.loadAPI("keyssi");
const crypto = openDSU.loadAPI('crypto');
const enclave = openDSU.loadAPI('enclave');
const baseURL = openDSU.loadAPI('system').getBaseURL();
const constants = require('../constants');
const dbName = "demiurge";
const lightDBEnclaveClient = enclave.initialiseLightDBEnclave(dbName);
const https = require('https');
const {URL} = require('url');
const path = require("path");
const {promises: fs} = require("fs");
const {makeRequest, generatePk} = require("../utils");
const urlModule = require("url");
const apiClient = require("../controllers/APIClient").getInstance();
const HEALTH_CHECK_TABLE = constants.HEALTH_CHECK_TABLE;
const Tasks = constants.HEALTH_CHECK_COMPONENTS;
const Status = constants.HEALTH_CHECK_STATUSES;

function getRandomResult() {
    const randomNum = Math.random();
    return randomNum < 0.8 ? constants.HEALTH_CHECK_STATUSES.SUCCESS : constants.HEALTH_CHECK_STATUSES.FAILED;
}

function getRandomRepairStatus() {
    const randomNum = Math.random();
    return randomNum < 0.8 ? constants.HEALTH_CHECK_STATUSES.REPAIRED : constants.HEALTH_CHECK_STATUSES.FAILED_REPAIR;
}

function StatusController(server) {
    this.fixSecrets = () => {
        return getRandomRepairStatus();
    }
    this.getInstallInfo = () => {
        return {
            name: constants.HEALTH_CHECK_COMPONENTS.INSTALL_INFO,
            status: getRandomResult(),
            date: Date.now(),
            logs: "LOG      0x00  2024-07-10T05:54:10.454Z apihub/logg Logger      GET:/demiurge/assets/images/icons/arrow-right-short.svg 200 6.91ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.455Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-groups.svg 200 6.468ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-my-identities.svg 200 5.915ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-api-key.svg 200 5.199ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-audit.svg 200 4.732ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.457Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-system-status.svg 200 4.707ms\n" +
                "DEBUG    0x00  2024-07-10T05:54:10.457Z overwrite-r Logger      WARN     0x00  2024-07-10T05:54:10.457Z apihub"
        };
    }
    this.getSystemHealthInfo = () => {
        return {
            name: constants.HEALTH_CHECK_COMPONENTS.SYSTEM_HEALTH,
            status: getRandomResult(),
            date: Date.now(),
            logs: "LOG      0x00  2024-07-10T05:54:10.454Z apihub/logg Logger      GET:/demiurge/assets/images/icons/arrow-right-short.svg 200 6.91ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.455Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-groups.svg 200 6.468ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-my-identities.svg 200 5.915ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-api-key.svg 200 5.199ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-audit.svg 200 4.732ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.457Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-system-status.svg 200 4.707ms\n" +
                "DEBUG    0x00  2024-07-10T05:54:10.457Z overwrite-r Logger      WARN     0x00  2024-07-10T05:54:10.457Z apihub"
        };
    }
    this.getConfigsInfo = () => {
        return {
            name: constants.HEALTH_CHECK_COMPONENTS.CONFIGS_INFO,
            status: getRandomResult(),
            date: Date.now(),
            logs: "LOG      0x00  2024-07-10T05:54:10.454Z apihub/logg Logger      GET:/demiurge/assets/images/icons/arrow-right-short.svg 200 6.91ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.455Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-groups.svg 200 6.468ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-my-identities.svg 200 5.915ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-api-key.svg 200 5.199ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-audit.svg 200 4.732ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.457Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-system-status.svg 200 4.707ms\n" +
                "DEBUG    0x00  2024-07-10T05:54:10.457Z overwrite-r Logger      WARN     0x00  2024-07-10T05:54:10.457Z apihub"
        }
    }
    this.checkWallets = () => {
        return {
            name: constants.HEALTH_CHECK_COMPONENTS.WALLETS,
            status: getRandomResult(),
            date: Date.now(),
            logs: "LOG      0x00  2024-07-10T05:54:10.454Z apihub/logg Logger      GET:/demiurge/assets/images/icons/arrow-right-short.svg 200 6.91ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.455Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-groups.svg 200 6.468ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-my-identities.svg 200 5.915ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-api-key.svg 200 5.199ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.456Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-audit.svg 200 4.732ms\n" +
                "LOG      0x00  2024-07-10T05:54:10.457Z apihub/logg Logger      GET:/demiurge/assets/images/icons/menu-system-status.svg 200 4.707ms\n" +
                "DEBUG    0x00  2024-07-10T05:54:10.457Z overwrite-r Logger      WARN     0x00  2024-07-10T05:54:10.457Z apihub"
        }
    }
    this.fixWallet = () => {
        return getRandomRepairStatus();
    }

    function getComponentPk(healthCheckPK, componentName) {
        return healthCheckPK + "_" + componentName;
    }

    const markNeverExecutedCheck = async (checkType, healthCheckId) => {
        const record = await $$.promisify(lightDBEnclaveClient.getRecord)($$.SYSTEM_IDENTIFIER, HEALTH_CHECK_TABLE, healthCheckId);
        await $$.promisify(lightDBEnclaveClient.insertRecord)($$.SYSTEM_IDENTIFIER, healthCheckId, componentPk, {
            data: {
                status: Status.NEVER_EXECUTED,
                name: checkType
            }
        });
    };

    const initiateCheck = async (checkType, healthCheckId) => {
        await $$.promisify(lightDBEnclaveClient.insertRecord)($$.SYSTEM_IDENTIFIER, checkType, healthCheckId, {
            status: Status.IN_PROGRESS,
            name: checkType
        });
    }

    const markCheckCompletion = async (checkType, healthCheckId, checkData) => {
        await $$.promisify(lightDBEnclaveClient.updateRecord)($$.SYSTEM_IDENTIFIER, checkType, healthCheckId, checkData);
    }

    const getSecretsStatus = (secretsReport) => {
        if (secretsReport.corruptSecrets.fixable.length > 0 || secretsReport.corruptSecrets.notFixable.length > 0) {
            return Status.FAILED;
        }

        return Status.SUCCESS;
    }

    this.checkSecrets = async (req, res) => {
        let checkId = req.body.healthCheckId;
        if (!checkId) {
            checkId = Date.now().toString();
        }
        await initiateCheck(Tasks.SECRETS, checkId);
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain");
        res.end(checkId);
        const encryptionKeys = process.env.SSO_SECRETS_ENCRYPTION_KEY ? process.env.SSO_SECRETS_ENCRYPTION_KEY.split(",") : undefined;
        let encryptionKey = encryptionKeys ? encryptionKeys[0].trim() : undefined;
        encryptionKey = $$.Buffer.from(encryptionKey, "base64");
        const secretsPath = path.join(server.rootFolder, "external-volume", "secrets");
        const secretsFiles = await fs.readdir(secretsPath);
        const report = {
            corruptSecrets: {
                fixable: [],
                notFixable: []
            }
        }
        for (let i = 0; i < secretsFiles.length; i++) {
            const secretFile = secretsFiles[i];
            const secretPath = path.join(secretsPath, secretFile);
            const secretContent = await fs.readFile(secretPath);
            try {
                crypto.decrypt(secretContent, encryptionKey);
            } catch (e) {
                // remove extension from secret file
                const extensionIndex = secretFile.lastIndexOf(".");
                const secretsContainer = secretFile.substring(0, extensionIndex);
                const obj = {
                    secretsContainer,
                    reason: "Failed to decrypt secrets container"
                }
                report.corruptSecrets.fixable.push(obj);
            }
        }

        let result = {
            status: getSecretsStatus(report),
            healthCheckId: checkId
        }
        result.report = report;
        await markCheckCompletion(Tasks.SECRETS, checkId, result);
    }

    function isArraySuffix(a, b) {
        // Step 1: Check if 'b' is longer than 'a'
        if (b.length > a.length) {
            return false;
        }

        // Step 2: Calculate the starting index 'k' in 'a'
        let k = a.length - b.length;

        // Step 3: Compare elements from 'a[k]' to 'a[a.length - 1]' with 'b'
        for (let i = 0; i < b.length; i++) {
            if (a[k + i] !== b[i]) {
                return false;
            }
        }

        return true;
    }

    const checkAnchorsForDomain = async (domain) => {
        const domainPath = path.join(server.rootFolder, "external-volume", "domains", domain);
        const anchorsPath = path.join(domainPath, "anchors");
        const anchorFiles = await fs.readdir(anchorsPath);
        let report;
        for (let i = 0; i < anchorFiles.length; i++) {
            const anchorFile = anchorFiles[i];
            const anchorPath = path.join(anchorsPath, anchorFile);
            const anchorContent = await fs.readFile(anchorPath);
            const lines = anchorContent.toString().split("\n");
            //remove the last line if it is empty
            if (lines[lines.length - 1] === "") {
                lines.pop();
            }
            const missingBrickMaps = [];
            for (let j = 0; j < lines.length; j++) {
                const encodedHashLink = lines[j];
                if (encodedHashLink === "") {
                    const obj = {
                        anchor: anchorFile,
                        reason: "Empty hashlink",
                        line: j
                    }
                    if (!report) {
                        report = {
                            corruptAnchors: {
                                fixable: [],
                                notFixable: []
                            }
                        }
                    }
                    report.corruptAnchors.notFixable.push(obj);
                    // break from the second loop
                    break;
                }
                const decodedHashLink = crypto.decodeBase58(encodedHashLink).toString();
                let signedHashlinkSSI;
                try {
                    signedHashlinkSSI = keySSISpace.parse(decodedHashLink);
                } catch (e) {
                    const obj = {
                        anchor: anchorFile,
                        reason: `Failed to parse hashlink ${decodedHashLink}`,
                        line: j
                    }
                    if (!report) {
                        report = {
                            corruptAnchors: {
                                fixable: [],
                                notFixable: []
                            }
                        }
                    }
                    report.corruptAnchors.notFixable.push(obj);
                    break;
                }
                const originalHash = signedHashlinkSSI.getHash();
                let hash = originalHash;
                let counter = 0;
                let found = false;
                while (counter < 5 && !found) {
                    const hashPrefix = hash.substring(0, 2);
                    const brickPath = path.join(domainPath, "brick-storage", hashPrefix, originalHash);
                    try {
                        await fs.access(brickPath);
                        found = true;
                    } catch (e) {
                        if (e.code === "ENOENT") {
                            counter++;
                            // change the case of the hash to accommodate for case-sensitive file systems
                            if (counter === 1) {
                                hash = hash.substring(0, 1).toLowerCase() + hash.substring(1, 2).toUpperCase() + hash.substring(2);
                            } else if (counter === 2) {
                                hash = hash.substring(0, 1).toUpperCase() + hash.substring(1, 2).toLowerCase() + hash.substring(2);
                            } else if (counter === 3) {
                                hash = hash.toLowerCase();
                            } else {
                                hash = hash.toUpperCase();
                            }
                        } else {
                            const obj = {
                                anchor: anchorFile,
                                reason: `Error accessing brickPath ${brickPath}`,
                                line: j
                            }
                            if (!report) {
                                report = {
                                    corruptAnchors: {
                                        fixable: [],
                                        notFixable: []
                                    }
                                }
                            }
                            report.corruptAnchors.notFixable.push(obj);
                            break;
                        }
                    }
                }
                if (!found) {
                    missingBrickMaps.push(encodedHashLink);
                }

                // if the missing brickmaps are at the end of the anchor file, the anchor file can be fixed
                if (missingBrickMaps.length > 0) {
                    if (isArraySuffix(lines, missingBrickMaps)) {
                        const obj = {
                            anchor: anchorFile,
                            reason: `Missing brickMaps at the end of the anchor file`,
                            line: lines.length - missingBrickMaps.length,
                            missingBrickMaps: missingBrickMaps
                        }
                        if (!report) {
                            report = {
                                corruptAnchors: {
                                    fixable: [],
                                    notFixable: []
                                }
                            }
                        }
                        report.corruptAnchors.fixable.push(obj);
                    }
                }

            }
        }
        return report;
    }

    const getStatusForAnchorReport = (report) => {
        for (let domain in report) {
            if (report[domain].corruptAnchors && (report[domain].corruptAnchors.notFixable.length > 0 || report[domain].corruptAnchors.fixable.length > 0)) {
                return Status.FAILED;
            }
        }

        return Status.SUCCESS;
    }

    this.checkAnchoring = async (req, res) => {
        let checkId = req.body.healthCheckId;
        if (!checkId) {
            checkId = Date.now().toString();
        }
        await initiateCheck(Tasks.ANCHORING, checkId);
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain");
        res.end(checkId);
        const domainsPath = path.join(server.rootFolder, "external-volume", "domains");
        try {
            await fs.access(domainsPath);
        } catch (e) {
            console.error(e);
            res.statusCode = 500;
            res.end(`Error`);
            return;
        }
        const domains = await fs.readdir(domainsPath);
        const reports = {};
        let hasErrors = false;
        for (let i = 0; i < domains.length; i++) {
            const domain = domains[i];
            try {
                const report = await checkAnchorsForDomain(domain);
                reports[domain] = report || "";
            } catch (e) {
                reports[domain] = e;
                hasErrors = true;
            }

        }

        let result = {
            status: hasErrors ? Status.FAILED : getStatusForAnchorReport(reports),
            healthCheckId: checkId
        }
        result.report = reports;
        await markCheckCompletion(Tasks.ANCHORING, checkId, result);
    }

    const checkBrickingForDomain = async (domain) => {
        const domainPath = path.join(server.rootFolder, "external-volume", "domains", domain);
        const bricksPath = path.join(domainPath, "brick-storage");
        const bricksSubfolders = await fs.readdir(bricksPath);
        const corruptBricks = [];
        // iterate over each subfolder
        for (let i = 0; i < bricksSubfolders.length; i++) {
            const subfolder = bricksSubfolders[i];
            const subfolderPath = path.join(bricksPath, subfolder);
            const brickFiles = await fs.readdir(subfolderPath);
            // iterate over each brick file
            for (let j = 0; j < brickFiles.length; j++) {
                const brickFile = brickFiles[j];
                const brickPath = path.join(subfolderPath, brickFile);
                try {
                    await fs.access(brickPath);
                    const brickContent = await fs.readFile(brickPath);
                    // compute content hash
                    const hash = crypto.sha256(brickContent);
                    if (hash !== brickFile) {
                        corruptBricks.push({
                            brick: brickFile,
                            reason: "Invalid hash"
                        });
                    }
                } catch (e) {
                    if (e.code === "ENOENT") {
                        corruptBricks.push({
                            brick: brickFile,
                            reason: "File not found"
                        });
                    }
                }
            }
        }
        return corruptBricks;
    }

    const getBrickingStatus = async (brickingReport) => {
        for (let domain in brickingReport) {
            if (brickingReport[domain].length > 0) {
                return Status.FAILED;
            }
        }

        return Status.SUCCESS;
    }

    this.checkBricking = async (req, res) => {
        let checkId = req.body.healthCheckId;
        if (!checkId) {
            checkId = Date.now().toString();
        }
        await initiateCheck(Tasks.BRICKING, checkId);
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain");
        res.end(checkId);
        const domainsPath = path.join(server.rootFolder, "external-volume", "domains");
        const domains = await fs.readdir(domainsPath);
        const reports = {};
        let hasErrors = false;
        for (let i = 0; i < domains.length; i++) {
            const domain = domains[i];
            try {
                const report = await checkBrickingForDomain(domain);
                reports[domain] = report || "";
            } catch (e) {
                reports[domain] = e;
                hasErrors = true;
            }
        }

        const status = hasErrors ? Status.FAILED : await getBrickingStatus(reports);
        let result = {
            status: status,
            healthCheckId: checkId,
            report: reports
        };
        await markCheckCompletion(Tasks.BRICKING, checkId, result);
    }

    this.checkDatabases = async (req, res) => {
        if (typeof req.body !== 'object') {
            res.status(400).json({ error: "Invalid request body format" });
            return;
        }

        const tables = Array.isArray(req.body.tables) ? req.body.tables : [];
        const checkId = typeof req.body.healthCheckId === 'string' ? req.body.healthCheckId : Date.now().toString();

        await initiateCheck(Tasks.DATABASES, checkId);
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain");
        res.end(checkId);

        let status = undefined;
        const reports = {};
        if (tables.length === 0) {
            status = Status.FAILED;
            reports["no-tables"] = {
                message: "No tables found.",
            }
        }

        if (status !== Status.FAILED) {
            const encodedQuery = encodeURIComponent("__timestamp > 0");
            for (const table of Object.values(tables)) {
                try {
                    const r = await $$.promisify(lightDBEnclaveClient.filter)($$.SYSTEM_IDENTIFIER, table, undefined);
                    reports[table] = {
                        status: "OK",
                        dataCount: r.length
                    };
                } catch (e) {
                    status = Status.FAILED;
                    reports[table] = e;
                }
            }
        }

        const result = {
            status: status || Status.SUCCESS,
            healthCheckId: checkId,
            report: reports
        };
        await markCheckCompletion(Tasks.DATABASES, checkId, result);
    }

    this.checkProducts = async (req, res) => {
        console.log("Check Products headers=", req.headers)
        const checkId = req.body?.healthCheckId || Date.now().toString();
        await initiateCheck(Tasks.PRODUCTS, checkId);
        const reports = {};
        try {
            // Step 1: Make an authorized request to list products URL
            const encodedQuery = encodeURIComponent("__timestamp > 0");
            const getProductsURL = baseURL + `/integration/listProducts?query=${encodedQuery}`;
            const headers = {
                'Authorization': req.headers.authorization,
                'Content-Type': 'application/json'
            };

            const responseProducts = await makeRequest(getProductsURL, 'GET', headers);

            const products = responseProducts.body;

            const urlParts = urlModule.parse(req.url, true);
            const {healthCheckRunId} = urlParts.query;
            let record = await $$.promisify(lightDBEnclaveClient.getRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, healthCheckRunId);

            // Step 2: For each object, send a request to list languages URL
            const totalProducts = products.length;
            reports["dataCount"] = totalProducts;
            for (let i = 0; i < totalProducts; i++) {
                const product = products[i];
                const getLanguagesURL = baseURL + `/integration/listProductLangs/${product.productCode}/leaflet`;
                const responseLanguages = await makeRequest(getLanguagesURL, 'GET', headers);

                const languages = responseLanguages.body;


                // Step 3: If the response is an array of language codes, send a request to URL
                if (Array.isArray(languages) && languages.length > 0) {
                    for (const lang of languages) {
                        const getLeafletsURL = baseURL + `/integration/epi/${product.productCode}/${lang}/leaflet`;
                        await makeRequest(getLeafletsURL, 'GET', headers);
                    }
                }

                // Update progress
                const data = {status: `In Progress: ${((i + 1) / totalProducts * 100).toFixed(0)}%`}
                if (i === totalProducts - 1) {
                    data.status = constants.HEALTH_CHECK_STATUSES.SUCCESS;
                }
                await $$.promisify(lightDBEnclaveClient.updateRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, healthCheckRunId, {data});
            }

            const result = {
                status: Status.SUCCESS,
                healthCheckId: checkId,
                report: reports
            };
            await markCheckCompletion(Tasks.PRODUCTS, checkId, result);
            res.statusCode = 200;
            // If no errors occur, log success message
            res.end("Products check successful, no issues found");
        } catch (error) {
            const result = {
                status: Status.FAILED,
                healthCheckId: checkId,
                report: error
            };
            await markCheckCompletion(Tasks.PRODUCTS, checkId, result);

            res.statusCode = 500;
            let message = "";
            // Handle errors by logging the appropriate message
            if (error.statusCode) {
                message = `Request to ${error.url} failed with status ${error.statusCode} and response body ${error.body}`;
            } else {
                message = `Request failed with error.`;
            }
            res.setHeader("Content-Type", "text/plain");
            res.end("Failed to check products");
        }
    }

    this.checkBatches = async (req, res) => {
        const checkId = req.body?.healthCheckId || Date.now().toString();
        try {
            await initiateCheck(Tasks.BATCHES, checkId);
            // Step 1: Make an authorized request to list batches URL
            const getBatchesURL = baseURL + '/integration/listBatches?query=__timestamp%20%3E%200';
            const headers = {
                // 'Authorization': req.headers.authorization,
                'Content-Type': 'application/json'
            };

            const responseBatches = await makeRequest(getBatchesURL, 'GET', headers);

            const batches = responseBatches.body;
            const urlParts = urlModule.parse(req.url, true);
            const {healthCheckRunId} = urlParts.query;
            let record = await $$.promisify(lightDBEnclaveClient.getRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, healthCheckRunId);

            // Step 2: For each object, send a request to list languages URL
            const totalBatches = batches.length;
            for (let i = 0; i < totalBatches; i++) {
                const batch = batches[i];
                const getLanguagesURL = baseURL + `/integration/listProductLangs/${batch.productCode}/${batch.batchNumber}/leaflet`;
                const responseLanguages = await makeRequest(getLanguagesURL, 'GET', headers);

                const languages = responseLanguages.body;

                // Step 3: If the response is an array of language codes, send a request to URL
                if (Array.isArray(languages) && languages.length > 0) {
                    for (const lang of languages) {
                        const getLeafletsURL = baseURL + `/integration/epi/${batch.productCode}/${batch.batchNumber}/${lang}/leaflet`;
                        await makeRequest(getLeafletsURL, 'GET', headers);
                    }
                }

                // Update progress
                data.status = `Progress: ${((i + 1) / totalBatches * 100).toFixed(0)}%`;
                if (i === totalBatches - 1) {
                    data.status = constants.HEALTH_CHECK_STATUSES.SUCCESS;
                }
                await $$.promisify(lightDBEnclaveClient.updateRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, healthCheckRunId, {data: data});
            }

            const result = {
                status: Status.SUCCESS,
                healthCheckId: checkId,
                report: "OK"
            };
            await markCheckCompletion(Tasks.BATCHES, checkId, result);

            res.statusCode = 200;
            res.setHeader("Content-Type", "text/plain");
            // If no errors occur, log success message
            res.end("Batches check successful, no issues found");
            // const checkData = { batches: batches, status: Status.COMPLETED, name: Task.CHECK_BATCHES };
            // resolve(markCheckCompletion(Task.CHECK_BATCHES, healthCheckId, checkData));
        } catch (error) {
            const result = {
                status: Status.FAILED,
                healthCheckId: checkId,
                report: error
            };
            await markCheckCompletion(Tasks.BATCHES, checkId, result);

            res.statusCode = 500;
            let message = "";
            // Handle errors by logging the appropriate message
            if (error.statusCode) {
                message = `Request to ${error.url} failed with status ${error.statusCode} and response body ${error.body}`;
            } else {
                message = `Request failed with error.`;
            }
            console.debug(message);
            res.setHeader("Content-Type", "text/plain");
            res.end("Failed to check batches");
        }
    }

    const getCheckStatusForAllComponents = async (healthCheckId) => {
        const statuses = {};
        for (let component in constants.HEALTH_CHECK_COMPONENTS) {
            const componentName = constants.HEALTH_CHECK_COMPONENTS[component];
            let record;
            try {
                record = await $$.promisify(lightDBEnclaveClient.getRecord)($$.SYSTEM_IDENTIFIER, componentName, healthCheckId);
            } catch (e) {
                //ignored and handled below
            }
            if (record) {
                statuses[componentName] = record;
            } else {
                statuses[componentName] = {status: Status.NEVER_EXECUTED};
            }
        }

        return statuses;
    }

    this.getCheckStatus = async (req, res) => {
        const {healthCheckId} = req.params;
        // get checkType from query params
        const urlParts = urlModule.parse(req.url, true);
        const {checkType} = urlParts.query;
        // if checkType is undefined get records for all check types
        if (!checkType) {
            const statuses = await getCheckStatusForAllComponents(healthCheckId);
            res.statusCode = 200;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify(statuses));
            return;
        }
        let record;
        try {
            record = await $$.promisify(lightDBEnclaveClient.getRecord)($$.SYSTEM_IDENTIFIER, checkType, healthCheckId);
        } catch (e) {
            //ignored and handled below
        }
        if (record) {
            res.statusCode = 200;
            res.setHeader("Content-type", "application/json");
            res.end(JSON.stringify(record));
            return;
        }
        res.statusCode = 200;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({status: Status.NEVER_EXECUTED}));
    }

    const startCheckForComponent = async (checkType, healthCheckId) => {
        // make the first letter of checkType uppercase
        const component = checkType.charAt(0).toUpperCase() + checkType.slice(1);
        return await apiClient[`check${component}`](healthCheckId);
    }

    this.startHealthCheck = async (req, res) => {
        // start health check for all components
        let pk = Date.now().toString();
        req.body = {
            pk
        };
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain");
        res.end(pk);

        let objectData = {
            status: constants.HEALTH_CHECK_STATUSES.IN_PROGRESS,
            date: Date.now(),
            pk: pk
        }
        await $$.promisify(lightDBEnclaveClient.insertRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, pk, {data: objectData});

        const components = Object.values(constants.HEALTH_CHECK_COMPONENTS);
        const promises = components.map((component) => startCheckForComponent(component, pk));

        try {
            await Promise.allSettled(promises);
        } catch (e) {
            objectData.status = constants.HEALTH_CHECK_STATUSES.FAILED;
            await $$.promisify(lightDBEnclaveClient.updateRecord)($$.SYSTEM_IDENTIFIER, constants.HEALTH_CHECK_TABLE, pk, {data: objectData});
        }

    }

    this.generateFailure = async (req, res) => {
        const FailuresRegistry = require('./FailuresRegistry');
        const failuresRegistry = new FailuresRegistry();
        const action = req.body.action;
        const component = req.body.component;
        try {
            await failuresRegistry.execute(component, action, server.rootFolder, req.body.args);
            res.statusCode = 200;
            res.end("Success");
        } catch (e) {
            console.error(e);
            res.statusCode = 500;
            res.end("Fail");
        }
    }

    this.listChecks = async (checkType) => {

    }


    this.getCheckResult = async (checkType, date) => {

    }
}

function getInstance(server) {
    return new StatusController(server);
}

module.exports = {
    getInstance
};
