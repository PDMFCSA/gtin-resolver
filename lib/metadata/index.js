const GTIN_SSI = require("../GTIN_SSI");
const path = require("path");
const fs = require('fs');
const utils = require("../leaflet-web-api/leafletUtils");
const {EPI_TYPES} = require("../constants/constants");
const XMLDisplayService = require("../services/XMLDisplayService/XMLDisplayService");
const languageServiceUtils = require("../utils/Languages");
const LeafletInfoService = require("../services/LeafletInfoService");
const {buildQueryParams} = require("../utils/buildQueryParams");
const CACHE_FILE = "dsuMetadata";

/**
 * @swagger
 * tags:
 *   - name: Leaflet Metadata
 *     description: retrieve ePI leaflet metadata
 *
 * components:
 *   schemas:
 *     ErrorResponse:
 *       type: object
 *       properties:
 *         code:
 *           type: string
 *           description: Error code
 *           example: "002"
 *
 *     BatchData:
 *       type: object
 *       description: >
 *         Detailed batch-specific information.
 *         Only returned when the batchNumber parameter is provided.
 *       properties:
 *         productCode:
 *           type: string
 *           example: "99456789010121"
 *         batchNumber:
 *           type: string
 *           example: "B2025XZ"
 *         epiProtocol:
 *           type: string
 *           example: "v1"
 *         lockId:
 *           type: string
 *           example: "3wRXHqN56ecLTtQbPY5caqR1N5iJQp8jXPzxsPU5WPws"
 *         expiryDate:
 *           type: string
 *           example: "251231"
 *         batchRecall:
 *           type: boolean
 *           example: false
 *         packagingSiteName:
 *           type: string
 *           example: "PharmaPack Solutions GmbH"
 *         importLicenseNumber:
 *           type: string
 *           example: "IMPORT-789456-BR"
 *         manufacturerName:
 *           type: string
 *           example: "Global Pharmaceuticals SA"
 *         dateOfManufacturing:
 *           type: string
 *           example: "230601"
 *         manufacturerAddress1:
 *           type: string
 *           example: "Industriestrasse 45"
 *         manufacturerAddress2:
 *           type: string
 *           example: "Pharma Park, Building C"
 *         manufacturerAddress3:
 *           type: string
 *           example: "Floor 3, Department B"
 *         manufacturerAddress4:
 *           type: string
 *           example: "Section 2.1"
 *         manufacturerAddress5:
 *           type: string
 *           example: "Basel, 4057, Switzerland"
 *       required:
 *         - productCode
 *         - batchNumber
 *         - expiryDate
 *       nullable: true
 *
 *     StrengthItem:
 *       type: object
 *       properties:
 *         substance:
 *           type: string
 *           description: Active pharmaceutical ingredient
 *           example: "Paracetamol"
 *         strength:
 *           type: string
 *           description: Dosage strength with unit
 *           example: "100mg"
 *       required:
 *         - strength
 *
 *     MarketItem:
 *       type: object
 *       properties:
 *         marketId:
 *           type: string
 *           description: ISO country code of the market
 *           example: "IN"
 *         nationalCode:
 *           type: string
 *           description: National product code
 *           example: "NPL123"
 *         mahName:
 *           type: string
 *           description: Marketing Authorization Holder name
 *           example: "IndiaMAH"
 *         legalEntityName:
 *           type: string
 *           description: Legal entity name
 *           example: "IndiaMAHEntity"
 *         mahAddress:
 *           type: string
 *           description: MAH registered address (HTML escaped)
 *           example: "456 Pharma Avenue"
 *       required:
 *         - marketId
 *
 *     ProductData:
 *       type: object
 *       properties:
 *         productCode:
 *           type: string
 *           example: "99456789010121"
 *         epiProtocol:
 *           type: string
 *           example: "v1"
 *         lockId:
 *           type: string
 *           example: "6Mf6jMHuXHpv15yfbRQu9YjvuB3GgqV5cRFsWjtQEL3x"
 *         internalMaterialCode:
 *           type: string
 *           example: "INC_PROD_001"
 *         inventedName:
 *           type: string
 *           example: "PharmaProduct"
 *         nameMedicinalProduct:
 *           type: string
 *           example: "PharmaMedicinalProduct"
 *         productRecall:
 *           type: boolean
 *           example: false
 *         strengths:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/StrengthItem'
 *         markets:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/MarketItem'
 *         productPhoto:
 *           type: string
 *           example: "./assets/icons/product_image_placeholder.svg"
 *         batchData:
 *           $ref: '#/components/schemas/BatchData'
 *           nullable: true
 *       required:
 *         - productCode
 *         - inventedName
 *         - nameMedicinalProduct
 *
 *     LanguageItem:
 *       type: object
 *       properties:
 *         label:
 *           type: string
 *           example: "Arabic"
 *         value:
 *           type: string
 *           example: "ar"
 *         nativeName:
 *           type: string
 *           example: "العربية"
 *
 *     DocumentType:
 *       type: object
 *       description: >
 *         Dynamic keys represent epi market, as a country code in ISO 3166-1 alpha-2 format (e.g. "PT", "BR").
 *         Special key "unspecified" is used for documents not tied to a specific market.
 *       properties:
 *         unspecified:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/LanguageItem'
 *         US:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/LanguageItem'
 *         BR:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/LanguageItem'
 *       additionalProperties:
 *         type: array
 *         items:
 *           $ref: '#/components/schemas/LanguageItem'
 *
 *     MetadataResponse:
 *       type: object
 *       properties:
 *         productData:
 *           $ref: '#/components/schemas/ProductData'
 *         availableDocuments:
 *           type: object
 *           description: >
 *             Contains document types with their available languages.
 *             Possible keys: "leaflet", "prescribingInfo", or empty if no documents available.
 *           properties:
 *             leaflet:
 *               $ref: '#/components/schemas/DocumentType'
 *               nullable: true
 *             prescribingInfo:
 *               $ref: '#/components/schemas/DocumentType'
 *               nullable: true
 *           additionalProperties: false
 */


function getMetadata(server){
    if (server.allowFixedUrl) {
        //let's make the fixedURL middleware aware of our endpoints
        server.allowFixedUrl("/metadata/");
    }

    const logger = $$.getLogger("metadata", "getMetadata");
    // const lokiEnclaveFacadeModule = require("loki-enclave-facade");
    // const createLokiEnclaveFacadeInstance = lokiEnclaveFacadeModule.createCouchDBEnclaveFacadeInstance;
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
            const indexMetadata = new Set();
            for (let epiType of Object.values(EPI_TYPES)) {
                const constSSI = GTIN_SSI.createGTIN_SSI(epiDomain, undefined, gtin);
                const leafletXmlService = new XMLDisplayService(null, constSSI, model, epiType);

                // const langsFromUnspecifiedMarket = await leafletXmlService.mergeAvailableLanguages();
                const {indexes, languages: langsFromUnspecifiedMarket} = await leafletXmlService.getAvailableLanguageMetadata(gtin, batchNumber, epiType);
                indexes.forEach((v) => indexMetadata.add(v));
                const ePIsByLang = await utils.getEPIMarketsForProductAsync(epiDomain, gtin, epiType);
                const ePIsByMarket = {};
                for (const [lang, countries] of Object.entries(ePIsByLang)) {
                    for (const country of countries) {
                        if (!ePIsByMarket[country])
                            ePIsByMarket[country] = [];
                        ePIsByMarket[country].push(languageServiceUtils.getLanguageAsItemForVMFromCode(lang));
                        indexMetadata.add(buildQueryParams(gtin, undefined, lang, epiType, country));
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
                availableDocuments: documentsMetadata,
                availableKeys: [...indexMetadata]
            }));
        } catch (err) {
            logger.info(0x103, err.message);
            return sendResponse(response, 500, err.message);
        }
    }


    /**
     * @swagger
     * /metadata/leaflet/{domain}:
     *   get:
     *     summary: Get leaflet metadata by domain and GTIN
     *     description: Retrieves leaflet metadata with validation of GTIN and optional batch number
     *     operationId: getDSUMetadataHandler
     *     tags: [Leaflet Metadata]
     *     parameters:
     *       - name: domain
     *         in: path
     *         description: The EPI domain identifier
     *         required: true
     *         schema:
     *           type: string
     *           example: "local.epi"
     *       - name: gtin
     *         in: query
     *         description: 14-digit GTIN number
     *         required: true
     *         schema:
     *           type: string
     *           pattern: '^\d{14}$'
     *       - name: batch
     *         in: query
     *         description: Batch number (optional)
     *         required: false
     *         schema:
     *           type: string
     *           nullable: true
     *     responses:
     *       '200':
     *         description: Successfully retrieved leaflet metadata
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/MetadataResponse'
     *             example:
     *               productData:
     *                 productCode: "99456789010121"
     *                 epiProtocol: "v1"
     *                 lockId: "6Mf6jMHuXHpv15yfbRQu9YjvuB3GgqV5cRFsWjtQEL3x"
     *                 internalMaterialCode: ""
     *                 inventedName: "PharmaInventedName"
     *                 nameMedicinalProduct: "PharmaMedicinalProduct"
     *                 productRecall: false
     *                 strengths: []
     *                 markets: []
     *                 productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                 batchData: null
     *               availableDocuments:
     *                 leaflet:
     *                   unspecified:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                   AT:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                     - label: "Arabic"
     *                       value: "ar"
     *                       nativeName: "العربية"
     *       '400':
     *         description: Invalid request parameters
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             examples:
     *               missingGtin:
     *                 value:
     *                   code: "002"
     *               invalidGtin:
     *                 value:
     *                   code: "003"
     *               unknownProduct:
     *                 value:
     *                   code: "001"
     *       '500':
     *         description: Internal server error
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: "Internal server error occurred"
     *       '529':
     *         description: Server busy processing request
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: "Server busy finding metadata"
     */
    server.get("/metadata/leaflet/:domain", getDSUMetadataHandler);
    /**
     * @swagger
     * /metadata/leaflet/{domain}/{subdomain}:
     *   get:
     *     summary: Internal metadata redirection endpoint
     *     description: Internal endpoint that handles subdomain redirection. Backward compatibility.
     *     operationId: redirectMetadataRequest
     *     tags: [Leaflet Metadata]
     *     parameters:
     *       - name: domain
     *         in: path
     *         description: The EPI domain identifier
     *         required: true
     *         schema:
     *           type: string
     *           example: "local.epi"
     *       - name: subdomain
     *         in: path
     *         description: The EPI subdomain identifier
     *         required: true
     *         schema:
     *           type: string
     *           example: "local.epi.sub"
     *       - name: gtin
     *         in: query
     *         description: 14-digit GTIN number
     *         required: true
     *         schema:
     *           type: string
     *           pattern: '^\d{14}$'
     *       - name: batch
     *         in: query
     *         description: Batch number (optional)
     *         required: false
     *         schema:
     *           type: string
     *           nullable: true
     *     responses:
     *       '200':
     *         description: Successfully retrieved leaflet metadata
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/MetadataResponse'
     *             example:
     *               productData:
     *                 productCode: "99456789010121"
     *                 epiProtocol: "v1"
     *                 lockId: "6Mf6jMHuXHpv15yfbRQu9YjvuB3GgqV5cRFsWjtQEL3x"
     *                 internalMaterialCode: ""
     *                 inventedName: "PharmaInventedName"
     *                 nameMedicinalProduct: "PharmaMedicinalProduct"
     *                 productRecall: false
     *                 strengths: []
     *                 markets: []
     *                 productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                 batchData: null
     *               availableDocuments:
     *                 leaflet:
     *                   unspecified:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                   AT:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                     - label: "Arabic"
     *                       value: "ar"
     *                       nativeName: "العربية"
     *       '400':
     *         description: Invalid request parameters
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             examples:
     *               missingGtin:
     *                 value:
     *                   code: "002"
     *               invalidGtin:
     *                 value:
     *                   code: "003"
     *               unknownProduct:
     *                 value:
     *                   code: "001"
     *       '500':
     *         description: Internal server error
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: "Internal server error occurred"
     *       '529':
     *         description: Server busy processing request
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: "Server busy finding metadata"
     */
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