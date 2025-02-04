const errMap = require("opendsu").loadApi("m2dsu").getErrorsMap();

const errorUtils = require("../mappings/errors/errorUtils");
errorUtils.addMappingError("MVP1_RESTRICTED");

const itemValidator = function (messageValue, schemaObj, schemaKey) {

    if (messageValue && typeof messageValue === "string") {
        messageValue = messageValue.trim()
    }

    if ((typeof messageValue === "string") && messageValue && !validateForHTMLTags(messageValue)) {
        invalidFields.push({field: schemaKey, message: `HTML tags are not allowed`});
        return;
    }

    /*    if (!schemaObj.required && (schemaObj.type === "array" || schemaObj.type === "object") && !messageValue) {
            invalidFields.push({
                field: schemaKey,
                message: `Wrong type. Found ${typeof messageValue} , expected ${schemaObj.type}`
            });
            return;
        }*/

    if (!schemaObj.required && !messageValue) {
        return;
    }

    if (schemaObj.required && (!messageValue || ((typeof messageValue === "string") && !messageValue.trim()))) {
        invalidFields.push({field: schemaKey, message: `Required field`});
        return;
    }

    if (schemaObj.regex && !schemaObj.regex.test(messageValue)) {
        if (schemaKey === "messageTypeVersion") {
            invalidFields.push({field: schemaKey, message: `Wrong message version.`});
            return;
        }
        if (schemaKey === "marketId") {
            invalidFields.push({field: schemaKey, message: `not recognized`});
            return;
        }

        invalidFields.push({field: schemaKey, message: `Invalid format`});

        return;
    }
    
    if(schemaObj.type.toLowerCase().trim() === "date") {
        function isValidDate(date) {
            return date instanceof Date && !isNaN(date);
        }
        if(!isValidDate(new Date(messageValue))) {
            invalidFields.push({
                field: schemaKey,
                message: `Wrong date or date format`
            });
        }  
        return;
    } 

    if (schemaObj.type === "batchDate") {
        let resultDate;
        let wrongLastDay = false;
        if (messageValue.length === 6) {
            let year = "20" + messageValue.slice(0, 2);
            let month = messageValue.slice(2, 4);
            let day = messageValue.slice(4, 6);
            if (day === "00") {
                day = "01";
            }
            resultDate = new Date(`${year}/${month}/${day}`);
            wrongLastDay = resultDate.getMonth() + 1 !== parseInt(month);
        }
        if (!resultDate || resultDate.toString() === "Invalid Date" || wrongLastDay) {
            invalidFields.push({
                field: schemaKey,
                message: `Wrong date or date format`
            });
        }

        return;
    }

    if ((schemaObj.type !== "array" && schemaObj.type !== typeof messageValue) || (schemaObj.type === "array" && !Array.isArray(messageValue))) {
        invalidFields.push({
            field: schemaKey,
            message: `Wrong type. Found ${typeof messageValue} , expected ${schemaObj.type}`
        });
        return;
    }
}

const schemaParser = function (message, schema) {
    const schemaObject = schema.properties;
    const schemaKeys = Object.keys(schemaObject);
    for (let i = 0; i < schemaKeys.length; i++) {
        const schemaKey = schemaKeys[i];
        if (schemaObject[schemaKey].type === "object") {
            if (!message[schemaKey]) {
                itemValidator(message[schemaKey], schemaObject[schemaKey], schemaKey);
            } else {
                schemaParser(message[schemaKey], schemaObject[schemaKey]);
            }
        }
        if (schemaObject[schemaKey].type === "array") {
            if (Array.isArray(message[schemaKey]) && message[schemaKey].length >= 0) {
                message[schemaKey].forEach(msg => {
                    if (schemaObject[schemaKey].items.type === "object" && typeof msg === "object") {
                        schemaParser(msg, schemaObject[schemaKey].items)
                    } else {
                        itemValidator(msg, schemaObject[schemaKey].items, schemaKey);
                    }

                })
            } else if (schemaObject[schemaKey].required) {
                if (message[schemaKey]) {
                    invalidFields.push({
                        field: schemaKey,
                        message: `Wrong type. Found ${typeof message[schemaKey]} , expected ${schemaObject.type}`
                    });
                    return;
                } else {
                    invalidFields.push({field: schemaKey, message: `Required field`});
                    return;
                }
            } else if (typeof message[schemaKey] !== "undefined") {
                invalidFields.push({
                    field: schemaKey,
                    message: `Wrong type. Found ${typeof message[schemaKey]} , expected ${schemaObject.type}`
                });
                return;
            }
        }

        if (schemaObject[schemaKey].type !== "object" && schemaObject[schemaKey].type !== "array") {
            if (!message[schemaKey] && schemaObject[schemaKey].defaultValue) {
                message[schemaKey] = schemaObject[schemaKey].defaultValue;
            } else {
                itemValidator(message[schemaKey], schemaObject[schemaKey], schemaKey);
            }
        }
    }

    Object.keys(message).forEach(payloadKey => {
        if (!schemaKeys.includes(payloadKey))
            invalidFields.push({
                field: payloadKey,
                message: `Invalid field: "${payloadKey}", this field is not defined in the schema.`
            });
    });
}
let invalidFields;
const validateMsgOnSchema = function (message, schema) {
    invalidFields = [];
    schemaParser(message, schema);
    if (invalidFields.length > 0) {
        return {
            valid: false, invalidFields: invalidFields
        }
    }
    return {
        valid: true
    }
}

async function validateMessageOnSchema(message, schema) {
    const msgValidation = validateMsgOnSchema(message, schema);
    if (!msgValidation.valid) {
        message.invalidFields = msgValidation.invalidFields;
        throw errMap.newCustomError(errMap.errorTypes.INVALID_MESSAGE_FORMAT, msgValidation.invalidFields);
    }
    return msgValidation;
}

const MVP1_DISABLED_DEFAULT_VALUES_MAP = {
    "flagEnableAdverseEventReporting": false,
    "flagEnableACFProductCheck": false,
    "healthcarePractitionerInfo": "SmPC",
    "snValid": [],
    "snRecalled": [],
    "snDecom": [],
    "flagEnableBatchRecallMessage": false,
    "flagEnableSNVerification": false,
    "flagEnableEXPVerification": false,
    "flagEnableExpiredEXPCheck": true,
    "recallMessage": "",
    "batchMessage": "",
    "flagEnableACFBatchCheck": false,
    "acdcAuthFeatureSSI": "",
    "acfBatchCheckURL": false,
    "snValidReset": false,
    "snRecalledReset": false,
    "snDecomReset": false
}

async function validateMVP1Values(message, messageType) {
    const featManager = require("./../DSUFabricFeatureManager.js");
    if (await featManager.isFeatureEnabledAsync("02")) {
        //for demo system we need to escape any MVP1 validation
        return;
    }

    let invalidFields = [];
    if (messageType === "videos") {
        throw errMap.newCustomError(errMap.errorTypes.MVP1_RESTRICTED, "videos");
    }
    let msg = message[messageType];
    Object.keys(MVP1_DISABLED_DEFAULT_VALUES_MAP).forEach(key => {
        if (msg[key] && JSON.stringify(msg[key]) !== JSON.stringify(MVP1_DISABLED_DEFAULT_VALUES_MAP[key])) {
            invalidFields.push({
                field: key,
                message: `MVP1 wrong filed value, expected: ${MVP1_DISABLED_DEFAULT_VALUES_MAP[key]}`
            });
        }
    })
    if (invalidFields.length > 0) {
        message.invalidFields = invalidFields;
        throw errMap.newCustomError(errMap.errorTypes.INVALID_MESSAGE_FORMAT, invalidFields);
    }
}

function validateGTIN(gtinValue) {
    const gtinMultiplicationArray = [3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3];
    const pattern = /^\d+$/;
    if (!pattern.test(gtinValue)) {
        return {isValid: false, message: "GTIN should be a numeric value"};
    }
    let gtinDigits = gtinValue.split("");

    // TO DO this check is to cover all types of gtin. For the moment we support just 14 digits length. TO update also in leaflet-ssapp
    /*
    if (gtinDigits.length !== 8 && gtinDigits.length !== 12 && gtinDigits.length !== 13 && gtinDigits.length !== 14) {

      return {isValid: false, message: "GTIN length should be 8, 12, 13 or 14"};
    }
    */

    if (gtinDigits.length !== 14) {
        return {isValid: false, message: "GTIN length should be 14"};
    }
    let j = gtinMultiplicationArray.length - 1;
    let reszultSum = 0;
    for (let i = gtinDigits.length - 2; i >= 0; i--) {
        reszultSum = reszultSum + gtinDigits[i] * gtinMultiplicationArray[j];
        j--;
    }
    let validDigit = Math.floor((reszultSum + 10) / 10) * 10 - reszultSum;
    if (validDigit === 10) {
        validDigit = 0;
    }
    if (gtinDigits[gtinDigits.length - 1] != validDigit) {
        return {isValid: false, message: "Invalid GTIN. Last digit should be " + validDigit};
    }

    return {isValid: true, message: "GTIN is valid"};
}

function validateForHTMLTags(text) {
    let htmlTagPattern = /<(br|basefont|hr|input|source|frame|param|area|meta|!DOCTYPE).*?>|<(a|abbr|acronym|address|applet|article|aside|audio|b|bdi|bdo|big|blockquote|body|button|canvas|caption|center|cite|code|colgroup|command|datalist|dd|del|details|dfn|dialog|dir|div|dl|dt|em|embed|fieldset|figcaption|figure|font|footer|form|frameset|head|header|hgroup|h1|h2|h3|h4|h5|h6|html|i|iframe|ins|kbd|keygen|label|legend|li|map|mark|menu|meter|nav|noframes|noscript|object|ol|optgroup|output|p|pre|progress|q|rp|rt|ruby|s|samp|script|section|select|small|span|strike|strong|style|sub|summary|sup|table|tbody|td|textarea|tfoot|th|thead|time|title|tr|track|tt|u|ul|var|video).*?>/i;
    // Check for HTML tags
    if (htmlTagPattern.test(text)) {
        return false;
    }
    return true;
}

module.exports = {
    validateMessageOnSchema,
    validateGTIN,
    validateMVP1Values
}

