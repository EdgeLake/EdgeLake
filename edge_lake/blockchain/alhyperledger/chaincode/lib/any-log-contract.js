/*
* AnyLog Smart Contract
*
*
*/
'use strict';

const {Contract} = require('fabric-contract-api');

class AnyLogContract extends Contract {
    async InitLedger(ctx) {
        console.info('Empty Policy Key List Initialized');
    }

    async Insert(ctx, policy_key, policy) {
        let d = await ctx.stub.getState(policy_key);
        if (d.length > 0) {
            throw new Error(`The policy key ${policy_key} exists`);
        }
        ctx.stub.putState(policy_key, Buffer.from(policy));
        console.log(`Insert Policy: ${policy_key} ----> SUCCESS`);

        return JSON.stringify('SUCCESS');
    }

    async DeletePolicy(ctx, policy_key) {
        const exists = await this.PolicyExists(ctx, policy_key);
        if (!exists) {
            throw new Error(`Policy ${policy_key} does not exist`);
        }
        return ctx.stub.deleteState(policy_key);

    }

    async PolicyExists(ctx, policy_key) {
        const policyJSON = await ctx.stub.getState(policy_key);
        return policyJSON && policyJSON.length > 0;
    }

    // ReadAsset returns the asset stored in the world state with given id.
    async Get(ctx, policy_key) {
        const policy = await ctx.stub.getState(policy_key); // get the asset from chaincode state
        if (!policy || policy.length === 0) {
            throw new Error(`The policy_key ${policy_key} does not exist`);
        }
        return policy.toString();
    }

    async GetAll(ctx) {
        const allResults = [];
        // range query with empty string for startKey and endKey does an open-ended query of all assets in the chaincode namespace.
        const iterator = await ctx.stub.getStateByRange('', '');
        let result = await iterator.next();
        while (!result.done) {
            const strValue = Buffer.from(result.value.value.toString()).toString('utf8');
            let record;
            try {
                record = JSON.parse(strValue);
            } catch (err) {
                console.log(err);
                record = strValue;
            }
            allResults.push({ policy: record });
            //allResults.push({ policy_key: result.value.key, policy: record });
            result = await iterator.next();
        }
        return JSON.stringify(allResults);
    }

}

module.exports = AnyLogContract;
