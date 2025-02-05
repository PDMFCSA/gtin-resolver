const ModelBase = require("./ModelBase.js");
const GTIN_SSI = require("../../GTIN_SSI.js");
const constants = require("../../constants/constants.js");
const {EVENTS} = require("../utils/Events");
const XMLDisplayService = require("../../services/XMLDisplayService/XMLDisplayService");
const {LEAFLET_XML_FILE_NAME, EPI_MOUNT_PREFIX} = require("../../constants/constants");

function Product(enclave, domain, subdomain, gtin, version) {

    const MUTABLE_MOUNTING_POINT = `/product`;
    let instance = new ModelBase(enclave, domain, subdomain, gtin);

    //all the data that needs to be serialized as JSON needs to be added to the instance as soon as possible
    instance.productCode = gtin;
    //there is version specific paths and logic that may need to be carefully treated
    instance.epiProtocol = `v${version}`;

    instance.getJSONStoragePath = () => {
        return `/product.epi_${instance.epiProtocol}`;
    }

    instance.getLeafletStoragePath = (language, type, epiMarket) => {
        let path = `/${type}/${language}`;
        if (epiMarket)
            path = `/${EPI_MOUNT_PREFIX}/${type}/${language}/${epiMarket}`;
        return path;
    }

    instance.getLeafletFilePath = (language, type, epiMarket) => {
        const baseName = epiMarket ? `${LEAFLET_XML_FILE_NAME}` : `${type}`;
        const fileName = baseName.endsWith(".xml") ? baseName : `${baseName}.xml`;
        return `${instance.getLeafletStoragePath(language, type, epiMarket)}/${fileName}`;
    }

    instance.getMutableMountingPoint = () => {
        return constants.PRODUCT_DSU_MOUNT_POINT;
    }

    instance.getPathSSIData = () => {
        return {
            path: `0/${instance.productCode}`,
            domain: subdomain
        };
    }

    instance.getGTINSSI = () => {
        return GTIN_SSI.createGTIN_SSI(domain, subdomain, instance.productCode);
    }

    let persist = instance.persist;
    instance.persist = async (auditContext,language) => {
        let diffs = await persist.call(instance, auditContext,language);
        try{
            //we don't wait for the request to be finalized because of the delays between fixedUrl and lightDB
            $$.promisify(require("./../../mappings/utils.js").activateGtinOwnerFixedUrl)(undefined, domain, gtin,null,language);
            $$.promisify(require("./../../mappings/utils.js").activateLeafletFixedUrl)(undefined, domain, gtin,null,language);
        }catch(err){
            //ignore them for the moment.
        }
        return diffs;
    }

    instance.addPhoto = async (photoData) => {
        let eventRecorder = await instance.getEventRecorderInstance(instance.getGTINSSI());
        let fileExtension = ".png"; //for now is hardcoded to png, but it should reflect the info from the base64 data encoding...
        eventRecorder.register(EVENTS.WRITE, getImageMountingPoint(fileExtension), photoData);
    }

    const getImageMountingPoint = (fileExtension) => {
        return `/photo${fileExtension}`;
    }

    instance.getPhoto = async (dsuVersion) => {
        const imageMountingPoint = getImageMountingPoint(".png");
        const dsu = await instance.loadMutableDSUInstance(dsuVersion);
        return await dsu.readFileAsync(imageMountingPoint);
    }

    instance.deletePhoto = async () => {
        let eventRecorder = await instance.getEventRecorderInstance(instance.getGTINSSI());
        let fileExtension = ".png"; //for now is hardcoded to png, but it should reflect the info from the base64 data encoding...
        eventRecorder.register(EVENTS.DELETE, `${MUTABLE_MOUNTING_POINT}/photo${fileExtension}`);
    }

    instance.listMarkets = async (epiType) => {
        const constants = require("../utils/constants");
        const XMLDisplayService = require("../../services/XMLDisplayService/XMLDisplayService.js");
        const simulatedModel = {networkName: domain, product: {gtin}};
        let xmlDisplayService = new XMLDisplayService(undefined, instance.getGTINSSI(), simulatedModel, epiType, undefined);
        return $$.promisify(xmlDisplayService.getAvailableMarketsForProduct, xmlDisplayService)();
    }

    instance.getPathOfPathSSI = () => {
        const path = `0/${instance.productCode}`;
        return {
            path: path,
            domain: subdomain
        };
    }

    const loadMetadata = instance.loadMetadata;
    instance.loadMetadata = async () => {
        await loadMetadata.call(instance);
        instance.inventedName = instance.inventedName || instance.name;
        instance.nameMedicinalProduct = instance.nameMedicinalProduct || instance.description;
        instance.productRecall = instance.productRecall || "";
    }
    return instance;
}

module.exports = Product;
