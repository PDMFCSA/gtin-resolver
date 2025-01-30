const openDSU = require("opendsu");
const resolver = openDSU.loadApi("resolver");
const GtinDSUFactory = require("./lib/GTIN_DSU_Factory");

resolver.registerDSUFactory("gtin", new GtinDSUFactory(resolver));
const {createGTIN_SSI, parseGTIN_SSI} = require("./lib/GTIN_SSI");
const DSUFabricFeatureManager = require("./lib/DSUFabricFeatureManager");
const LeafletFeatureManager = require("./lib/LeafletFeatureManager");
const LeafletInfoService = require("./lib/services/LeafletInfoService");
const DSUFabricUtils = require("./lib/utils/DSUFabricUtils");
const Languages = require("./lib/utils/Languages");
const Countries = require("./lib/utils/Countries");
const UploadTypes = require("./lib/utils/UploadTypes");
const XMLDisplayService = require("./lib/services/XMLDisplayService/XMLDisplayService");
const utils = require("./lib/utils/CommonUtils");
const logUtils = require("./lib/utils/LogUtils");
const validationUtils = require("./lib/utils/ValidationUtils");
const versionTransformer = require("./lib/EpiVersionTransformer")
const constants = require("./lib/constants/constants");
const EPISORClient = require("./lib/integrationAPIs/clients/EpiSORIntegrationClient");

module.exports = {
    createGTIN_SSI,
    parseGTIN_SSI,
    DSUFabricFeatureManager,
    LeafletFeatureManager,
    LeafletInfoService,
    DSUFabricUtils,
    UploadTypes,
    Languages,
    Countries,
    utils,
    logUtils,
    validationUtils,
    versionTransformer,
    constants,
    XMLDisplayService,
    loadApi: function (apiName) {
        switch (apiName) {
            case "mappings":
                return require("./lib/mappings");
            case "services":
                return require("./lib/services");
        }
    },
    getEPIMappingEngineForAPIHUB: function (server) {
        return require("./lib/apihubMappingEngine").getEPIMappingEngineForAPIHUB(server);
    },
    getEPIMappingEngineMessageResults: function (server) {
        return require("./lib/apihubMappingEngineMessageResults").getEPIMappingEngineMessageResults(server);
    },
    getWebLeaflet: function (server) {
        return require("./lib/leaflet-web-api").getWebLeaflet(server);
    },
    getGTINOwner: function (server) {
        return require("./lib/gtinOwner").getGTINOwner(server);
    },
    getMetadata: function (server) {
        return require("./lib/metadata").getMetadata(server);
    },
    getFixedUrl: function (server) {
        return require("./lib/fixed-urls").getFixedUrl(server);
    },
    getMessagesPipe: function () {
        const opendsu = require("opendsu");
        return opendsu.loadApi("m2dsu").getMessagesPipe();
    },
    getErrorsMap: function () {
        return require("opendsu").loadApi("m2dsu").getErrorsMap();
    },
    getMappingsUtils: function () {
        return require("./lib/utils/CommonUtils");
    },
    getEPISorClient: function (domain, subdomain, appName) {
        const EPISORClient = require("./lib/integrationAPIs/clients/EpiSORIntegrationClient");
        return EPISORClient.getInstance(domain, subdomain, appName);
    },
    getHealthCheckClient: function () {
        const HealthCheckClient = require("./lib/healthCheckAPIs/controllers/APIClient");
        return HealthCheckClient.getInstance();
    },
    getIntegrationAPIs: function (server) {
        return require("./lib/integrationAPIs")(server);
    },
    getHealthCheckAPIs: function (server) {
        return require("./lib/healthCheckAPIs")(server);
    }
}

