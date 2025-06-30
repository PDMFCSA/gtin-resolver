const XMLDisplayService = require("./../services/XMLDisplayService/XMLDisplayService");
const LeafletInfoService = require("./../services/LeafletInfoService");
const {EPI_TYPES} = require("../constants/constants");
const {getCountry} = require("../utils/Countries");
const utils = require("./leafletUtils");
const {buildQueryParams} = require("../utils/buildQueryParams");
const languageServiceUtils = require("../utils/Languages");
const GTIN_SSI = require("../GTIN_SSI");

/**
 * @swagger
 * tags:
 *   - name: Leaflets
 *     description: ePI leaflet data
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
 *       description: >-
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
 *           example: "251231"  # 31/12/2025
 *         batchRecall:
 *           type: boolean
 *           example: false
 *         packagingSiteName:
 *           type: string
 *           example: "PharmaPack Solutions Group"
 *         importLicenseNumber:
 *           type: string
 *           example: "IMPORT-789456-BR"
 *         manufacturerName:
 *           type: string
 *           example: "Global Pharmaceuticals Inc."
 *         dateOfManufacturing:
 *           type: string
 *           example: "230601"  # 01/06/2023
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
 *     LeafletResponse:
 *       type: object
 *       properties:
 *         resultStatus:
 *           type: string
 *           enum: [xml_found, has_no_leaflet, no_xml_for_lang]
 *         xmlContent:
 *           type: string
 *           nullable: true
 *         leafletImages:
 *           type: object
 *           nullable: true
 *         productData:
 *           $ref: '#/components/schemas/ProductData'
 *         availableLanguages:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/LanguageItem'
 *         availableEpiMarkets:
 *           type: object
 *           additionalProperties:
 *             type: array
 *             items:
 *               $ref: '#/components/schemas/LanguageItem'
 *         availableTypes:
 *           type: array
 *           items:
 *             type: string
 *           example: ["leaflet"]
 */

function getWebLeaflet(server) {

    server.registerAccessControlAllowHeaders(["epiprotocolversion", "X-Merck-APIkey", "X-api-key"]);
    if (server.allowFixedUrl) {
        //let's make the fixedURL middleware aware of our endpoints
        server.allowFixedUrl("/leaflets/");
    }

    const logger = $$.getLogger("leaflet-web-api", "getWebLeaflet");

    async function getLeafletHandler(request, response) {
        let domainName = request.params.domain;
        const isValidDomain = require("swarmutils").isValidDomain;
        if (!isValidDomain(domainName)) {
            logger.error("Domain validation failed", domainName);
            response.statusCode = 400;
            return response.end("Invalid domain");
        }

        // Sanitize and validate input parameters
        let leaflet_type = request.query.leaflet_type || "";
        let gtin = request.query.gtin || null;
        let lang = request.query.lang || "";
        let batchNumber = request.query.batch || null;
        const epiMarket = request.query?.epiMarket || null;

        // Validate gtin to be numeric and 14 characters
        if (gtin && (!/^\d{14}$/.test(gtin) || typeof gtin !== "string")) {
            logger.info(0x103, `Validation failed for gtin.length`);
            return sendResponse(response, 400, JSON.stringify({code: "002"}));
        }

        // Validate leaflet_type to only allow known values
        if (leaflet_type && !Object.values(EPI_TYPES).includes(leaflet_type)) {
            logger.info(0x103, `Unknown leaflet type: ${leaflet_type}`);
            return sendResponse(response, 400, "Unknown leaflet type. Please check API documentation.");
        }

        // Validate lang to allow only alphanumeric or hyphen (language code format)
        if (lang && !/^[a-zA-Z-0-9-]+$/.test(lang)) {
            logger.info(0x103, `Invalid language format: ${lang}`);
            return sendResponse(response, 400, "Invalid language format. Please check API documentation.");
        }

        if (epiMarket) {
            try {
                const country = getCountry(epiMarket);
                if (!country)
                    throw new Error(`Invalid ePI Market: ${epiMarket}`);
            } catch (e) {
                return sendResponse(response, 400, `Invalid ePI Market: ${epiMarket}.`);
            }
        }

        // Validate batchNumber if present
        if (batchNumber && batchNumber === "undefined") {
            batchNumber = null;
        }

        try {
            if (!gtin) {
                logger.info(0x103, `Missing required parameter <gtin>`);
                return sendResponse(response, 400, JSON.stringify({code: "002"}));
            }

            let validationResult = require("../utils/ValidationUtils").validateGTIN(gtin);
            if (validationResult && !validationResult.isValid) {
                logger.info(0x103, `Validation failed for gtin`);
                return sendResponse(response, 400, JSON.stringify({code: "003"}));
            }

            if (!leaflet_type) {
                logger.info(0x103, `Missing required parameter <leaflet_type>`);
                return sendResponse(response, 400, "leaflet_type is a required parameter. Please check API documentation.");
            }

            if (!lang) {
                logger.info(0x103, `Missing required parameter <lang>`);
                return sendResponse(response, 400, "lang is a required parameter. Please check API documentation.");
            }

            try {
                require("./../utils/Languages").getLanguageFromCode(lang);
            } catch (err) {
                logger.info(0x103, `Unable to handle lang: ${lang}`);
                return sendResponse(response, 400, "Unable to handle lang. Please check API documentation.");
            }

            let knownParams = ["leaflet_type", "gtin", "lang", "batch", "epiMarket", "fixedurlrequest"];
            let queryParams = Object.keys(request.query);
            for (let param of queryParams) {
                if (!knownParams.includes(param)) {
                    logger.debug(`Query contains invalid param`, param);
                    return server.makeLocalRequest("GET", `/leaflets/${domainName}?${buildQueryParams(gtin, batchNumber, lang, leaflet_type, epiMarket)}`, (err, content) => {
                        if (err) {
                            logger.error(0x100, "Error Object", err);
                            return sendResponse(response, 529, "Server busy reading gtin-only leaflet");
                        }
                        logger.debug(0x100, "Successfully returned content without invalid params");
                        return sendResponse(response, 200, content);
                    });
                }
            }

            const GTIN_SSI = require("../GTIN_SSI");
            const productGtinSSI = GTIN_SSI.createGTIN_SSI(domainName, undefined, gtin);
            const utils = require("./leafletUtils");
            let productKnown = false;
            try {
                productKnown = await utils.checkDSUExistAsync(productGtinSSI);
            } catch (err) {
                logger.info(0x103, `Unable to check Product DSU existence`);
                logger.error(err);
                return sendResponse(response, 529, "Server busy checking product existence");
            }

            if (!productKnown) {
                logger.info(0x103, `Gtin unknown`);
                return sendResponse(response, 400, JSON.stringify({code: "001"}));
            }

            let leafletInfo = await LeafletInfoService.init({gtin, batchNumber}, domainName);

            const model = {
                product: {gtin},
                networkName: domainName
            }
            let leafletXmlService = new XMLDisplayService(null, leafletInfo.gtinSSI, model, leaflet_type);

            let batchExists = false;
            let preventProductFallback = false;
            try {
                const batchGtinSSI = GTIN_SSI.createGTIN_SSI(domainName, undefined, gtin, batchNumber);
                batchExists = await utils.checkDSUExistAsync(batchGtinSSI);
                if (batchExists) {
                    let batchLanguages = await utils.getLanguagesForBatchAsync(domainName, gtin, batchNumber);
                    if (batchLanguages.indexOf(lang) !== -1) {
                        preventProductFallback = true;
                    } else {
                        let productLanguages = await utils.getLanguagesForProductAsync(domainName, gtin);
                        if (productLanguages.indexOf(lang) !== -1) {
                            preventProductFallback = false;
                        } else {
                            //if we get to this point, and we have some languages we need to skip the fallback and let the normal apis to do their stuff
                            if (batchLanguages.length) {
                                preventProductFallback = true;
                            }
                        }
                    }
                }
            } catch (err) {
                logger.info(0x103, `Unable to check Batch DSU existence`);
                logger.error(err);
                return sendResponse(response, 529, "Server busy checking batch existence");
            }

            if (!batchExists || (batchNumber && !preventProductFallback)) {
                let content;
                try {
                    let uri = `/leaflets/${domainName}?${buildQueryParams(gtin, undefined, lang, leaflet_type, epiMarket)}`;
                    content = await $$.promisify(server.makeLocalRequest, server)("GET", uri);
                } catch (err) {
                    if (err.httpCode && err.httpCode === 404) {

                    } else {
                        logger.error(0x100, "Error message", err.message);
                        logger.error(0x100, "Error Object", err);
                        logger.error(0x100, "Error Stack", err.stack);
                        return sendResponse(response, 529, `Server busy reading gtin only leaflet`);
                    }
                }

                if (content) {
                    logger.info(0x100, "Successfully returned content from redirect to gtin only url");
                    if (typeof content === "string")
                        content = JSON.parse(content);
                    if (content?.productData && !content.productData?.batchData)
                        try {
                            content.productData.batchData = await leafletInfo.getBatchClientModel();
                        } catch (e) {
                            content.productData.batchData = null
                            // if it fails it means we have to do our thing
                        }

                    if (!content?.availableLanguages) {
                        let constSSI = GTIN_SSI.createGTIN_SSI(domainName, undefined, gtin);
                        let leafletXmlService = new XMLDisplayService(null, constSSI, model, leaflet_type);
                        const langs = await leafletXmlService.mergeAvailableLanguages();
                        content.availableLanguages = Object.keys(langs || {}).map((lang) => {
                            return languageServiceUtils.getLanguageAsItemForVMFromCode(lang);
                        });
                    }

                    if (!content?.availableMarkets) {
                        const availableEpiMarkets = await utils.getEPIMarketsForProductAsync(domainName, gtin, leaflet_type);
                        const invertedMarkets = {};
                        for (const [lang, countries] of Object.entries(availableEpiMarkets)) {
                            for (const country of countries) {
                                if (!invertedMarkets[country])
                                    invertedMarkets[country] = [];
                                invertedMarkets[country].push(languageServiceUtils.getLanguageAsItemForVMFromCode(lang));
                            }
                        }
                        content.availableEpiMarkets = invertedMarkets;
                    }
                    if (!content?.availableTypes)
                        content.availableTypes = await utils.getEPITypesAsync(domainName, gtin);
                    return sendResponse(response, 200, JSON.stringify(content));
                }
            }

            if (lang && epiMarket) {
                let productData = await leafletInfo.getProductClientModel();
                return leafletXmlService.readXmlFileFromMarket(lang, epiMarket, async (err, xmlContent, pathBase, leafletImagesObj) => {
                    if (err) {
                        if (err.statusCode === 504) {
                            logger.error(0x100, "Error Object", err);
                            return sendResponse(response, 529, "System busy; please try again later");
                        }
                        let errMessage = `No available XML for gtin=${gtin} language=${lang} epiMarket=${epiMarket} leaflet type=${leaflet_type}`;
                        logger.info(0x103, errMessage);
                        return sendResponse(response, 200, JSON.stringify({
                            resultStatus: "has_no_leaflet",
                            epiMarket: epiMarket,
                            productData,
                        }));
                    }

                    // logger.audit(0x101, `Successful serving url ${response.req.url}`);
                    return sendResponse(response, 200, JSON.stringify({
                        resultStatus: "xml_found",
                        epiMarket: epiMarket,
                        xmlContent,
                        leafletImages: leafletImagesObj,
                        productData
                    }));
                });
            }

            leafletXmlService.readXmlFile(lang, async (err, xmlContent, pathBase, leafletImagesObj) => {
                if (err) {
                    if (err.statusCode === 504) {
                        logger.error(0x100, "Error Object", err);
                        return sendResponse(response, 529, "System busy; please try again later");
                    }
                    let errMessage = `No available XML for gtin=${gtin} language=${lang} leaflet type=${leaflet_type}`
                    if (batchNumber) {
                        errMessage = `${errMessage} batchNumber id=${batchNumber}`;
                    }

                    utils.getAvailableLanguagesForType(leafletInfo.gtinSSI, gtin, leaflet_type, async (langerr, availableLanguages) => {
                        if (langerr) {
                            logger.error(langerr);
                            logger.info(0x103, errMessage);
                            return sendResponse(response, 529, "System busy; please try again later");
                        }

                        let productData = await leafletInfo.getProductClientModel();

                        if (!availableLanguages || !availableLanguages.length) {

                            return sendResponse(response, 200, JSON.stringify({
                                resultStatus: "has_no_leaflet",
                                productData
                            }));
                        }
                        logger.info(0x100, "Sending alternative languages");
                        return sendResponse(response, 200, JSON.stringify({
                            resultStatus: "no_xml_for_lang",
                            availableLanguages: availableLanguages,
                            productData
                        }));
                    });
                } else {
                    let productData = await leafletInfo.getProductClientModel();
                    try {
                        let batchData = await leafletInfo.getBatchClientModel();
                        productData.batchData = batchData;
                    } catch (e) {
                        // gtin only case
                        productData.batchData = null;
                    }
                    // logger.audit(0x101, `Successful serving url ${response.req.url}`);
                    return sendResponse(response, 200, JSON.stringify({
                        resultStatus: "xml_found",
                        xmlContent,
                        leafletImages: leafletImagesObj,
                        productData
                    }));
                }
            }, lang)
        } catch (err) {
            logger.info(0x103, err.message);
            return sendResponse(response, 500, err.message);
        }
    }

    async function getLeafletDocumentsHandler(request, response) {
        let domainName = request.params.domain;
        const isValidDomain = require("swarmutils").isValidDomain;
        if (!isValidDomain(domainName)) {
            logger.error("Domain validation failed", domainName);
            response.statusCode = 400;
            return response.end("Invalid domain");
        }

        // Sanitize and validate input parameters
        let leaflet_type = request.query.leaflet_type || "";
        let gtin = request.query.gtin || null;

        // Validate gtin to be numeric and 14 characters
        if (gtin && (!/^\d{14}$/.test(gtin) || typeof gtin !== "string")) {
            logger.info(0x103, `Validation failed for gtin.length`);
            return sendResponse(response, 400, JSON.stringify({code: "002"}));
        }

        // Validate leaflet_type to only allow known values
        if (leaflet_type && !Object.values(EPI_TYPES).includes(leaflet_type)) {
            logger.info(0x103, `Unknown leaflet type: ${leaflet_type}`);
            return sendResponse(response, 400, "Unknown leaflet type. Please check API documentation.");
        }

    }

    // server.get("/leaflets/:domain/documents",getLeafletDocumentsHandler);

    /**
     * @swagger
     * /leaflets/{domain}:
     *   get:
     *     summary: Retrieve ePI leaflet information
     *     description: Returns ePI leaflet in XML format with product metadata
     *     tags: [Leaflets]
     *     parameters:
     *       - name: domain
     *         in: path
     *         required: true
     *         description: EPI domain identifier
     *         schema:
     *           type: string
     *           example: "local.epi"
     *       - name: leaflet_type
     *         in: query
     *         required: true
     *         description: Type of document to retrieve
     *         schema:
     *           type: string
     *           enum: [leaflet, prescribingInfo]
     *           example: "leaflet"
     *       - name: gtin
     *         in: query
     *         required: true
     *         description: 14-digit Global Trade Item Number
     *         schema:
     *           type: string
     *           pattern: '^\d{14}$'
     *       - name: lang
     *         in: query
     *         required: true
     *         description: Language code for the leaflet
     *         schema:
     *           type: string
     *           example: "en"
     *       - name: batch
     *         in: query
     *         description: Batch/lot number (optional)
     *         schema:
     *           type: string
     *           nullable: true
     *       - name: epiMarket
     *         in: query
     *         description: Target market country code (optional)
     *         schema:
     *           type: string
     *           nullable: true
     *     responses:
     *       '200':
     *         description: Successful response with leaflet data and complete product information
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/LeafletResponse'
     *             examples:
     *               xml_found:
     *                 value:
     *                   resultStatus: "xml_found"
     *                   xmlContent: "<xml>...</xml>"
     *                   leafletImages: {}
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData:
     *                       productCode: "09456789012344"
     *                       batchNumber: "B2025XZ"
     *                       epiProtocol: "v1"
     *                       lockId: "4q6oBxciFT7FDxZ5BHeNbJgetFpeNHMq2Rsu4gVmKbhy"
     *                       expiryDate: "251231"
     *                       batchRecall: false
     *                       packagingSiteName: "PharmaPack Solutions"
     *                       importLicenseNumber: "IMPORT-123456"
     *                       manufacturerName: "Global Pharmaceuticals"
     *                       dateOfManufacturing: "230601"
     *                       manufacturerAddress1: "123 Pharma Street"
     *                       manufacturerAddress2: "Building A"
     *                       manufacturerAddress3: "Floor 3"
     *                       manufacturerAddress4: "Section B"
     *                       manufacturerAddress5: "Basel, 4057, Switzerland"
     *                   availableLanguages:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                   availableEpiMarkets:
     *                     US:
     *                       - label: "English"
     *                         value: "en"
     *                         nativeName: "English"
     *                   availableTypes: ["leaflet"]
     *               no_xml_for_lang:
     *                 value:
     *                   resultStatus: "no_xml_for_lang"
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData: null
     *                   availableLanguages:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                     - label: "Spanish"
     *                       value: "es"
     *                       nativeName: "Español"
     *                   availableEpiMarkets: {}
     *                   availableTypes: ["leaflet", "prescribingInfo"]
     *               has_no_leaflet:
     *                 value:
     *                   resultStatus: "has_no_leaflet"
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData: null
     *                   availableLanguages: []
     *                   availableEpiMarkets: {}
     *                   availableTypes: []
     *       '400':
     *         description: Invalid request parameters
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             examples:
     *               invalid_gtin:
     *                 value:
     *                   code: "002"
     *               unknown_product:
     *                 value:
     *                   code: "001"
     *               invalid_params:
     *                 value:
     *                   code: "003"
     *       '500':
     *         description: Internal server error
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             example:
     *               code: "500"
     *       '529':
     *          description: Server busy processing request
     *          content:
     *            text/plain:
     *              schema:
     *                type: string
     *                example: "Server busy reading leaflet"
     */
    server.get("/leaflets/:domain", getLeafletHandler);


    /**
     * @swagger
     * /leaflets/{domain}/{subdomain}:
     *   get:
     *     summary: Retrieve ePI leaflet information
     *     description: Returns ePI leaflet in XML format with product metadata
     *     tags: [Leaflets]
     *     parameters:
     *       - name: domain
     *         in: path
     *         required: true
     *         description: EPI domain identifier
     *         schema:
     *           type: string
     *           example: "local.epi"
     *       - name: subdomain
     *         in: path
     *         required: true
     *         description: EPI subdomain identifier
     *         schema:
     *           type: string
     *       - name: leaflet_type
     *         in: query
     *         required: true
     *         description: Type of document to retrieve
     *         schema:
     *           type: string
     *           enum: [leaflet, prescribingInfo]
     *           example: "leaflet"
     *       - name: gtin
     *         in: query
     *         required: true
     *         description: 14-digit Global Trade Item Number
     *         schema:
     *           type: string
     *           pattern: '^\d{14}$'
     *       - name: lang
     *         in: query
     *         required: true
     *         description: Language code for the leaflet
     *         schema:
     *           type: string
     *           example: "en"
     *       - name: batch
     *         in: query
     *         description: Batch/lot number (optional)
     *         schema:
     *           type: string
     *           nullable: true
     *       - name: epiMarket
     *         in: query
     *         description: Target market country code (optional)
     *         schema:
     *           type: string
     *           nullable: true
     *     responses:
     *       '200':
     *         description: Successful response with leaflet data and complete product information
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/LeafletResponse'
     *             examples:
     *               xml_found:
     *                 value:
     *                   resultStatus: "xml_found"
     *                   xmlContent: "<xml>...</xml>"
     *                   leafletImages: {}
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData:
     *                       productCode: "09456789012344"
     *                       batchNumber: "B2025XZ"
     *                       epiProtocol: "v1"
     *                       lockId: "4q6oBxciFT7FDxZ5BHeNbJgetFpeNHMq2Rsu4gVmKbhy"
     *                       expiryDate: "251231"
     *                       batchRecall: false
     *                       packagingSiteName: "PharmaPack Solutions"
     *                       importLicenseNumber: "IMPORT-123456"
     *                       manufacturerName: "Global Pharmaceuticals"
     *                       dateOfManufacturing: "230601"
     *                       manufacturerAddress1: "123 Pharma Street"
     *                       manufacturerAddress2: "Building A"
     *                       manufacturerAddress3: "Floor 3"
     *                       manufacturerAddress4: "Section B"
     *                       manufacturerAddress5: "Basel, 4057, Switzerland"
     *                   availableLanguages:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                   availableEpiMarkets:
     *                     US:
     *                       - label: "English"
     *                         value: "en"
     *                         nativeName: "English"
     *                   availableTypes: ["leaflet"]
     *               no_xml_for_lang:
     *                 value:
     *                   resultStatus: "no_xml_for_lang"
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData: null
     *                   availableLanguages:
     *                     - label: "English"
     *                       value: "en"
     *                       nativeName: "English"
     *                     - label: "Spanish"
     *                       value: "es"
     *                       nativeName: "Español"
     *                   availableEpiMarkets: {}
     *                   availableTypes: ["leaflet", "prescribingInfo"]
     *               has_no_leaflet:
     *                 value:
     *                   resultStatus: "has_no_leaflet"
     *                   productData:
     *                     productCode: "09456789012344"
     *                     epiProtocol: "v1"
     *                     lockId: "ESiSt5ffvJPdcZzRC5EEEciRQfQcxv1qErfy362X7MVQ"
     *                     internalMaterialCode: ""
     *                     inventedName: "PharmaProduct"
     *                     nameMedicinalProduct: "MedicinalProductName"
     *                     productRecall: false
     *                     strengths: []
     *                     markets: []
     *                     productPhoto: "./assets/icons/product_image_placeholder.svg"
     *                     batchData: null
     *                   availableLanguages: []
     *                   availableEpiMarkets: {}
     *                   availableTypes: []
     *       '400':
     *         description: Invalid request parameters
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             examples:
     *               invalid_gtin:
     *                 value:
     *                   code: "002"
     *               unknown_product:
     *                 value:
     *                   code: "001"
     *               invalid_params:
     *                 value:
     *                   code: "003"
     *       '500':
     *         description: Internal server error
     *         content:
     *           application/json:
     *             schema:
     *               $ref: '#/components/schemas/ErrorResponse'
     *             example:
     *               code: "500"
     *       '529':
     *          description: Server busy processing request
     *          content:
     *            text/plain:
     *              schema:
     *                type: string
     *                example: "Server busy reading leaflet"
     */
    server.get("/leaflets/:domain/:subdomain", function (req, res) {
        let url = req.url.replace(`/${req.params.subdomain}`, "");
        logger.debug("Local searching for Leaflet without the extra params");
        return server.makeLocalRequest("GET", url, (err, content) => {
            if (err) {
                logger.error(0x100, "Error Object", err);
                return sendResponse(res, 529, `Server busy reading leaflet`);
            }
            logger.debug(0x100, "Successfully returned content after local redirect");
            return sendResponse(res, 200, content);
        });
    });

    server.get("/EMA/:gtin", function(req,res) {
        fetch(`https://epi-uat.ema.europa.eu/consuming/api/fhir/Bundle?carrierValue=${req.params.gtin}`, {
            method: 'GET',
            // Request headers
            headers: {
                'Cache-Control': 'no-cache',
            }
        }).then(async(response) => {
            let data = await response.json();
            if(response.ok){
                response =JSON.stringify(data)
                return sendResponse(res, 200,response );
            } else {
                return sendResponse(res, response.status, data.message ? data.message : "Fail to get EMA leaflet")
            }
        })
        .catch(err => {
            return sendResponse(res, 400, "Fail to get EMA leaflet")
        });
    });
}


function sendResponse(response, statusCode, message) {
    response.statusCode = statusCode;
    if (statusCode === 200) {
        response.setHeader("Content-type", "application/json");
    } else {
        response.setHeader("Content-Type", "text/plain");
    }
    response.end(message);
}

module.exports.getWebLeaflet = getWebLeaflet;
