const EVENTS = {
    WRITE: "WriteEvent",
    DELETE: "DeleteEvent"
}

function Event(path, content) {
    Object.assign(this, {path, content});
}

let getDiffsForAudit = (newData, prevData) => {
    if (prevData && (Array.isArray(prevData) || Object.keys(prevData).length > 0)) {
        let diffs = Object.keys(newData).reduce((diffs, key) => {
            if (JSON.stringify(prevData[key]) === JSON.stringify(newData[key])) return diffs
            return {
                ...diffs, [key]: {oldValue: prevData[key], newValue: newData[key]}
            }
        }, {})
        return diffs;
    } else {
        let diffs = {};
        for (let key of Object.keys(newData)) {
            if (Array.isArray(newData[key]) && !newData[key].length) {
                continue;
            }
            if (newData[key] === "") {
                continue;
            }
            diffs[key] = {oldValue: "", newValue: newData[key]};
        }
        return diffs;
    }
}

function WriteEvent(path, content, diff, ...args) {
    let instance = new Event(path, content, diff, ...args);

    instance.execute = async function (dsu) {
        let diffs;
        if (typeof diff !== "boolean") {
            diffs = diff;
        } else {
            if (diff) {
                let oldContent = null;
                let newContent = null;
                try {
                    oldContent = await dsu.readFileAsync(path);
                    oldContent = JSON.parse(oldContent.toString());

                } catch (err) {
                    oldContent = null;
                }
                newContent = JSON.parse(content);
                diffs = getDiffsForAudit(newContent, oldContent);
            } else {
                //no diffs...
            }
        }
        await dsu.writeFileAsync(path, content);
        return diffs ? {path, diffs} : undefined;
    }
    return instance;
}

function DeleteEvent(path, content, diff, ...args) {
    let instance = new Event(path, content, diff, ...args);

    instance.execute = async function (dsu) {
        let diffs;
        if (typeof diff !== "boolean") {
            diffs = diff;
        } else {
            if (diff) {
                let oldContent = null;
                try {
                    oldContent = await dsu.readFileAsync(path);
                    oldContent = JSON.parse(oldContent);
                    content = JSON.parse(content);
                } catch (err) {
                    oldContent = null;
                }

                diffs = getDiffsForAudit(null, oldContent);
            } else {
                //no diffs...
            }
        }
        await dsu.deleteAsync(path);
        return diffs ? {path, diffs} : undefined;
    }
    return instance;
}

const eventConstructors = {
    WriteEvent,
    DeleteEvent
}

function EventRecorder(getDSUFnc) {

    let events = [];
    this.register = function (operation, path, content, ...args) {
        let EventConstructor = eventConstructors[operation];
        events.push(new EventConstructor(path, content, ...args));
    }

    this.execute = async function (auditContext) {
        let {mutableDSU, batchId, immutableDSU} = await getDSUFnc();
        if (batchId) {
            let old = immutableDSU.batchInProgress;
            immutableDSU.batchInProgress = () => {
                immutableDSU.batchInProgress = old;
                return false;
            };
        }

        let version;
        let mutableBatchId = await mutableDSU.startOrAttachBatchAsync();
        let diffs = [];
        for (let event of events) {
            let operationDiff = await event.execute(mutableDSU);

            if(!operationDiff)
                continue;

            if(Object.keys(operationDiff.diffs) < 1)
                continue;

            if (operationDiff) {
                diffs.push(operationDiff);
            }
        }

        let promises = [];
        promises.push(mutableDSU.commitBatchAsync(mutableBatchId));
        if (batchId) {
            promises.push(immutableDSU.commitBatchAsync(batchId));
        }
        await Promise.all(promises);

        if (auditContext) {
            auditContext.diffs = diffs;
            try {
                version = await require("opendsu").loadApi("anchoring").getNextVersionNumberAsync(mutableDSU.getCreationSSI());
            } catch (err) {
                //todo: handle this error...
            }
            auditContext.version = version - 1;
        }
        return diffs;
    }
}

module.exports = {
    EventRecorder,
    EVENTS,
    getDiffsForAudit
}
