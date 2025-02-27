const GTIN_SSI = require("../GTIN_SSI");
const path = require("path");
const fs = require('fs');
const utils = require("../leaflet-web-api/leafletUtils");
const {EPI_TYPES} = require("../constants/constants");
const XMLDisplayService = require("../services/XMLDisplayService/XMLDisplayService");
const languageServiceUtils = require("../utils/Languages");
const LeafletInfoService = require("../services/LeafletInfoService");
const CACHE_FILE = "dsuMetadata";

function getMetadata(server){
    if (server.allowFixedUrl) {
        //let's make the fixedURL middleware aware of our endpoints
        server.allowFixedUrl("/metadata/");
    }

    const logger = $$.getLogger("metadata", "getMetadata");
    // const lokiEnclaveFacadeModule = require("loki-enclave-facade");
    // const createLokiEnclaveFacadeInstance = lokiEnclaveFacadeModule.createLokiEnclaveFacadeInstance;
    // const cachePath = path.join(server.rootFolder, "external-volume", "metadata", "cache");


    // const DATABASE_PERSISTENCE_TIMEOUT = 100;
    // let database;

    // if(!server.readOnlyModeActive){
    //     try {
    //         fs.accessSync(cachePath);
    //     } catch (e) {
    //         fs.mkdirSync(cachePath, {recursive: true});
    //     }
    //     database = createLokiEnclaveFacadeInstance(path.join(cachePath, CACHE_FILE), DATABASE_PERSISTENCE_TIMEOUT, lokiEnclaveFacadeModule.Adapters.PARTITIONED);
    // }

    // const METADATA_TABLE = "dsuMetadata";

    async function getDSUMetadataHandler(request, response) {
        let epiDomain = request.params.domain;
        const isValidDomain = require("swarmutils").isValidDomain;
        if(!isValidDomain(epiDomain)) {
            logger.error("Domain validation failed", epiDomain);
            response.statusCode = 400;
            return response.end("Fail");
        }

        // Sanitize and validate input parameters
        let gtin = request.query.gtin || null;
        let batchNumber = request.query.batch || null;

        // Validate gtin to be numeric and 14 characters
        if (gtin && (!/^\d{14}$/.test(gtin) || typeof gtin !== "string")) {
            logger.info(0x103, `Validation failed for gtin.length`);
            return sendResponse(response, 400, JSON.stringify({ code: "002" }));
        }

        // Validate batchNumber if present
        if (batchNumber && batchNumber === "undefined") {
            batchNumber = null;
        }

        // let metadataObj;
        // let metadataDBUrl = `/metadata/leaflet/${epiDomain}/${gtin}/${batchNumber}`

        // //Get from db
        // try {
        //     if(database) 
        //         metadataObj = await $$.promisify(database.getRecord)(undefined, METADATA_TABLE, metadataDBUrl);
        // } catch (err) {
        //     //exceptions on getting records from db are handled bellow
        // }

        // //Respond if object found in db
        // if (typeof metadataObj !== "object") {
        //     try {
        //         let dbObj = JSON.parse(metadataObj);
        //         if (dbObj !== "undefined") {
        //           return sendResponse(response, 200, metadataObj);
        //         }
        //     } catch (e) {
        //         //db record is invalid; continue with resolving request
        //     }
        // }


        try {

            if (!gtin) {
                logger.info(0x103, `Missing required parameter <gtin>`);
                return sendResponse(response, 400, JSON.stringify({ code: "002" }));
            }

            let validationResult = require("../utils/ValidationUtils").validateGTIN(gtin);

            if (validationResult && !validationResult.isValid) {
                logger.info(0x103, `Validation failed for gtin`);
                return sendResponse(response, 400, JSON.stringify({ code: "003" }));
            }


            
            // const productGtinSSI = GTIN_SSI.createGTIN_SSI(epiDomain, undefined, gtin);

            // let product = undefined;
            // try {
            //     product = await checkDSUExistAsync(productGtinSSI);
            // } catch(err) {
            //     logger.info(0x103, `Unable to check Product DSU existence`);
            //     logger.error(err);
            //     return sendResponse(response, 529, "Server busy checking product existence");
            // }

            // if(!product){
            //     logger.info(0x103, `Product unknown`);
            //     return sendResponse(response, 400, JSON.stringify({code: "001"}));
            // }
            
            // let batch = undefined;
            // try {
            //     const batchGtinSSI = GTIN_SSI.createGTIN_SSI(epiDomain, undefined, gtin, batchNumber);
            //     batch = await checkDSUExistAsync(batchGtinSSI);
            // } catch (err) {
            //     logger.info(0x103, `Unable to check Batch DSU existence`);
            //     logger.error(err);
            //     return sendResponse(response, 529, "Server busy checking batch existence");
            // }

            // if(!batch){
            //     logger.info(0x103, `Batch unknown`);
            //     return sendResponse(response, 400, JSON.stringify({code: "001"}));
            // }

            let leafletInfo = await LeafletInfoService.init({gtin, batchNumber}, epiDomain);
            const productData = await leafletInfo.getProductClientModel();

            const model = {product: {gtin}, batch: batchNumber, networkName: epiDomain};
            const documentsMetadata = {};
            for (let epiType of Object.values(EPI_TYPES)) {
                const constSSI = GTIN_SSI.createGTIN_SSI(epiDomain, undefined, gtin);
                const leafletXmlService = new XMLDisplayService(null, constSSI, model, epiType);

                const langsFromUnspecifiedMarket = await leafletXmlService.mergeAvailableLanguages();
                const ePIsByLang = await utils.getEPIMarketsForProductAsync(epiDomain, gtin, epiType);
                const ePIsByMarket = {};
                for (const [lang, countries] of Object.entries(ePIsByLang)) {
                    for (const country of countries) {
                        if (!ePIsByMarket[country])
                            ePIsByMarket[country] = [];
                        ePIsByMarket[country].push(languageServiceUtils.getLanguageAsItemForVMFromCode(lang));
                    }
                }

                if (Object.keys(langsFromUnspecifiedMarket).length || Object.keys(ePIsByLang).length) {
                    documentsMetadata[epiType] = {
                        ...(Object.keys(langsFromUnspecifiedMarket) ? {
                            unspecified: Object.keys(langsFromUnspecifiedMarket || {}).map((lang) => {
                                return languageServiceUtils.getLanguageAsItemForVMFromCode(lang);
                            })
                        } : {}),
                        ...ePIsByMarket
                    };
                }
            }

            // //Save object to db
            // if(!server.readOnlyModeActive){
            //     try {
            //       await $$.promisify(database.insertRecord)(undefined, METADATA_TABLE, metadataDBUrl, documentsMetadata);
            //     } catch (e) {
            //       logger.info(0x0, `Failed to cache metadata`, e.message);
            //       //failed to cache; continue without caching
            //     }
            // }

            if(!productData?.batchData)
                try{
                    productData.batchData = await leafletInfo.getBatchClientModel();
                  } catch(e){
                    productData.batchData = null
                    // if it fails it means we have to do our thing
                  }
                

            return sendResponse(response, 200, JSON.stringify({
                productData: productData,
                availableDocuments: documentsMetadata
            }));
        } catch (err) {
            logger.info(0x103, err.message);
            return sendResponse(response, 500, err.message);
        }
    }


    server.get("/metadata/leaflet/:domain", getDSUMetadataHandler);
    server.get("/metadata/leaflet/:domain/:subdomain", function(req, res){
      let url = req.url.replace(`/${req.params.subdomain}`, "");
      logger.debug("Local resolving of metadata without the extra params");
      return server.makeLocalRequest("GET", url, (err, content)=>{
        if(err){
          logger.error(0x100, err.message);
          return sendResponse(res, 529, `Server busy finding metadata` );
        }
        logger.debug(0x100, "Successfully returned metadata info after local redirect");
        return sendResponse(res, 200, content);
      });
    });
}

function sendResponse(response, statusCode, message) {
  response.statusCode = statusCode;
  if(statusCode === 200){
    response.setHeader("Content-type", "application/json");
  }else{
    response.setHeader("Content-Type", "text/plain");
  }
  response.end(message);
}

// async function checkDSUExistAsync(constSSI){
//     let apiName = "checkDSUExist";
//     let uid = constSSI.getIdentifier();
//     let result // = apiCache.getResult(uid, apiName);
//     if(result){
//         return result;
//     }
//     const resolver = require("opendsu").loadApi("resolver");
//     result = await $$.promisify(resolver.dsuExists)(constSSI);
//     // apiCache.registerResult(uid, apiName, result);
//     return result;
// }
module.exports.getMetadata = getMetadata;