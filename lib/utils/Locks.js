const opendsu = require("opendsu");
const lockApi = opendsu.loadApi("lock");
const Lock = require("./Lock");
const _lock = new Lock();

let count = 0;

async function acquireLock(resourceId, period) {
    const crypto = opendsu.loadApi("crypto");
    let secret = crypto.encodeBase58(crypto.generateRandom(32));

    let lockAcquired;
    await _lock.acquire();
    lockAcquired = await lockApi.lockAsync(resourceId, secret, period);
    _lock.release();

    if (!lockAcquired) {
        secret = undefined;
    }

    return secret;
}

async function releaseLock(resourceId, secret) {
    try {
        await lockApi.unlockAsync(resourceId, secret);
    } catch (err) {
        console.log('Release lock failed: ', err);
        console.log('The lock will be released after the expiration period set at the beginning.');
        //if the unlock fails, the lock will be released after the expiration period set at the beginning.
    }
}

module.exports = {acquireLock, releaseLock};