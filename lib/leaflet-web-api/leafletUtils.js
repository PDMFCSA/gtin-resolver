const XMLDisplayService = require("../services/XMLDisplayService/XMLDisplayService");
const GTIN_SSI = require("../GTIN_SSI");
const {EPI_TYPES} = require("../constants/constants");

function ApiCache(expirationTime){
    let cache = {};
    this.registerResult = function(uid, apiName, result){
        cache[uid+apiName] = {result, time:Date.now()};
    }

    this.getResult = function(uid, apiName){
        let potentialResult = cache[uid+apiName];
        if(!potentialResult){
            return;
        }
        if(Date.now()-expirationTime < potentialResult.time){
            return potentialResult.result;
        }
        delete cache[uid+apiName];
        return;
    }
}

let apiCache = new ApiCache(10*1000);

module.exports.getLanguagesForProductAsync = async function(domain, gtin){
    let apiName = "getLanguagesForProductAsync";
    let constSSI = GTIN_SSI.createGTIN_SSI(domain, undefined, gtin);
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if(result){
        return result;
    }
    const model = {
        product: {gtin},
        networkName: domain
    }
    let leafletXmlService = new XMLDisplayService(null, constSSI, model, "leaflet");
    result = await $$.promisify(leafletXmlService.getAvailableLanguagesFromPath, leafletXmlService)(constSSI, leafletXmlService.getProductPathToXmlType());
    apiCache.registerResult(uid, apiName, result);
    return result;
}

module.exports.getLanguagesForBatchAsync = async function(domain, gtin, batch){
    let apiName = "getLanguagesForBatchAsync";
    let constSSI = GTIN_SSI.createGTIN_SSI(domain, undefined, gtin, batch);
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if(result){
        return result;
    }
    const model = {
        product: {gtin},
        networkName: domain
    }
    let leafletXmlService = new XMLDisplayService(null, constSSI, model, "leaflet");
    result = await $$.promisify(leafletXmlService.getAvailableLanguagesFromPath, leafletXmlService)(constSSI, leafletXmlService.getBatchPathToXmlType());
    apiCache.registerResult(uid, apiName, result);
    return result;
}

/**
 * @param {string} domain - The domain name for which the ePI market information is requested.
 * @param {string} gtin - GTIN of the product.
 * @param {string} epiType - The type of ePI
 *
 * @returns {Promise<Object<string, string[]>>}
 */
module.exports.getEPIMarketsForProductAsync = async function(domain, gtin, epiType){
    let apiName = `getEPIMarketsForProductAsync_${epiType}`;
    let constSSI = GTIN_SSI.createGTIN_SSI(domain, undefined, gtin);
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if(result){
        return result;
    }
    const model = {
        product: {gtin},
        networkName: domain
    }
    let leafletXmlService = new XMLDisplayService(null, constSSI, model, epiType);
    result = await $$.promisify(leafletXmlService.getAvailableMarketsForProduct, leafletXmlService)();
    apiCache.registerResult(uid, apiName, result);
    return result;
}

module.exports.getEPITypesAsync = async function(domain, gtin) {
    let apiName = "getEPITypesAsync";
    let constSSI = GTIN_SSI.createGTIN_SSI(domain, undefined, gtin);
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if (result) {
        return result;
    }

    let availableLeafletTypes = [];
    const model = {product: { gtin }, networkName: domain};
    const epiTypes = Object.values(EPI_TYPES); //.filter((type) => type !== EPI_TYPES.SMPC);
    for (let epiType of epiTypes) {
        let leafletXmlService = new XMLDisplayService(null, constSSI, model, epiType);
        const noMarketLeafletsLanguages = await leafletXmlService.mergeAvailableLanguages();
        const marketLeaflets = await $$.promisify(leafletXmlService.hasMarketXMLAvailable, leafletXmlService)();
        if (Object.keys(noMarketLeafletsLanguages).length > 0 || marketLeaflets) {
            availableLeafletTypes.push(epiType);
        }
    }
    apiCache.registerResult(uid, apiName, availableLeafletTypes);
    return availableLeafletTypes;
};

module.exports.checkDSUExistAsync = async function(constSSI){
    let apiName = "checkDSUExist";
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if(result){
        return result;
    }
    const resolver = require("opendsu").loadApi("resolver");
    result = await $$.promisify(resolver.dsuExists)(constSSI);
    apiCache.registerResult(uid, apiName, result);
    return result;
}

module.exports.getAvailableLanguagesForType = function(constSSI, gtin, type, callback){
    let apiName = "getAvailableLanguagesForType"+type;
    let uid = constSSI.getIdentifier();
    let result = apiCache.getResult(uid, apiName);
    if(result){
        return callback(undefined, result.availableLanguages, result.availableMarkets);
    }

    const model = {
        product: {gtin},
        networkName: constSSI.getDLDomain()
    }
    let leafletXmlService = new XMLDisplayService(null, constSSI, model, type);

    leafletXmlService.getAvailableMarketsForProduct((err, availableMarkets) => {
        leafletXmlService.getAvailableLanguagesForXmlType((langerr, availableLanguages) => {
            if(langerr){
                return callback;
            }
            apiCache.registerResult(uid, apiName, {availableLanguages, availableMarkets});
            return callback(undefined, availableLanguages, availableMarkets);
        });
    });
}

module.exports.dsuExists = function (server, keySSI, gtin, useFastRoute=false, callback){
    if(typeof useFastRoute === "function"){
        callback = useFastRoute;
        useFastRoute = false;
    }

    function resolverCheckDSU(){
        const resolver = require("opendsu").loadApi("resolver");
        resolver.dsuExists(keySSI, callback);
    }

    if(useFastRoute){
        return server.makeLocalRequest("GET",`/gtinOwner/${keySSI.getDLDomain()}/${gtin}`, "", (err, response)=>{
            if(err || !response.domain){
                if(!err.cause){
                    //network error
                }
                return resolverCheckDSU();
            }

            return callback(undefined, true);
        });
    }

    resolverCheckDSU();
}

module.exports.dsuExistsAsync = $$.promisify(module.exports.dsuExists);