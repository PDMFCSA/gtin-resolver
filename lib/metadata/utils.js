const constants = require("../constants/constants.js");


async function getAvailableLanguagesForBatch(gtin) {

    const langs = await getAvailableLanguagesFromPath(gtin, `${constants.BATCH_DSU_MOUNT_POINT}/${constants.EPI_TYPES.LEAFLET}`);

            
    return langs;

}

async function getAvailableLanguagesFromPath(gtin, path){
    const resolver = require("opendsu").loadAPI("resolver");
    const dsu = await $$.promisify(resolver.loadDSU)(gtin);
    const result = await readLanguagesFromDSU(dsu, path)
    return result;
}


async function readLanguagesFromDSU(dsu, path) {
    const pskPath = require("swarmutils").path;

    let langFolders = await $$.promisify(dsu.listFolders)(path);
    let langs = [];

    for (let langFolder of langFolders) {
        let langFolderPath = pskPath.join(path, langFolder);
        let files = await $$.promisify(dsu.listFiles)(langFolderPath);
        let hasXml = files.find((item) => {
            return item.endsWith(".xml")
        })
        if (hasXml) {
            langs.push(langFolder)
        }
    }

    return langs;
}


module.exports = {
   getAvailableLanguagesForBatch
}