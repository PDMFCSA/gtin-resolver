const messageHeaderSchema = require("./../messageHeaderSchema");
let batchSchema = {
  "type": "object",
  "properties":
    {
      "payload": {
        "type": "object", "required": true,
        "properties": {
          "productCode": {"type": "string", "required": true},
          "batchNumber": {
            /*
            GS1 regex
            /^[!%-?A-Z_a-z\x22]{1,20}$/
            */
            "type": "string", "required": true, regex: /^[a-zA-Z0-9/-]{1,20}$/
          },
          "expiryDate": {"type": "batchDate", "required": true},
          "batchRecall": {"type": "boolean"},
          "packagingSiteName": {"type": "string"},
          "epiLeafletVersion": {"type": "number"},
          "flagEnableEXPVerification": {"type": "boolean"},
          "flagEnableExpiredEXPCheck": {"type": "boolean"},
          "batchMessage": {"type": "string"},
          "flagEnableBatchRecallMessage": {"type": "boolean"},
          "recallMessage": {"type": "string"},
          "flagEnableACFBatchCheck": {"type": "boolean"},
          "acfBatchCheckURL": {"type": "string"},
          "flagEnableSNVerification": {"type": "boolean"},
          // ACDC PATCH START
          "acdcAuthFeatureSSI": {"type": "string"},
          // ACDC PATCH END
          "snValidReset": {"type": "boolean"},
          "snValid": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
      }
    }
}
batchSchema.properties = {...messageHeaderSchema, ...batchSchema.properties};
module.exports = batchSchema
