/*
 * SPDX-License-Identifier: Apache-2.0
 */

'use strict';

const { ChaincodeStub, ClientIdentity } = require('fabric-shim');
const { AnyLogContract } = require('..');
const winston = require('winston');

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');

chai.should();
chai.use(chaiAsPromised);
chai.use(sinonChai);

class TestContext {

    constructor() {
        this.stub = sinon.createStubInstance(ChaincodeStub);
        this.clientIdentity = sinon.createStubInstance(ClientIdentity);
        this.logger = {
            getLogger: sinon.stub().returns(sinon.createStubInstance(winston.createLogger().constructor)),
            setLevel: sinon.stub(),
        };
    }

}

describe('AnyLogContract', () => {

    let contract;
    let ctx;

    beforeEach(() => {
        contract = new AnyLogContract();
        ctx = new TestContext();
        ctx.stub.getState.withArgs('1001').resolves(Buffer.from('{"value":"any log 1001 value"}'));
        ctx.stub.getState.withArgs('1002').resolves(Buffer.from('{"value":"any log 1002 value"}'));
    });

    describe('#anyLogExists', () => {

        it('should return true for a any log', async () => {
            await contract.anyLogExists(ctx, '1001').should.eventually.be.true;
        });

        it('should return false for a any log that does not exist', async () => {
            await contract.anyLogExists(ctx, '1003').should.eventually.be.false;
        });

    });

    describe('#createAnyLog', () => {

        it('should create a any log', async () => {
            await contract.createAnyLog(ctx, '1003', 'any log 1003 value');
            ctx.stub.putState.should.have.been.calledOnceWithExactly('1003', Buffer.from('{"value":"any log 1003 value"}'));
        });

        it('should throw an error for a any log that already exists', async () => {
            await contract.createAnyLog(ctx, '1001', 'myvalue').should.be.rejectedWith(/The any log 1001 already exists/);
        });

    });

    describe('#readAnyLog', () => {

        it('should return a any log', async () => {
            await contract.readAnyLog(ctx, '1001').should.eventually.deep.equal({ value: 'any log 1001 value' });
        });

        it('should throw an error for a any log that does not exist', async () => {
            await contract.readAnyLog(ctx, '1003').should.be.rejectedWith(/The any log 1003 does not exist/);
        });

    });

    describe('#updateAnyLog', () => {

        it('should update a any log', async () => {
            await contract.updateAnyLog(ctx, '1001', 'any log 1001 new value');
            ctx.stub.putState.should.have.been.calledOnceWithExactly('1001', Buffer.from('{"value":"any log 1001 new value"}'));
        });

        it('should throw an error for a any log that does not exist', async () => {
            await contract.updateAnyLog(ctx, '1003', 'any log 1003 new value').should.be.rejectedWith(/The any log 1003 does not exist/);
        });

    });

    describe('#deleteAnyLog', () => {

        it('should delete a any log', async () => {
            await contract.deleteAnyLog(ctx, '1001');
            ctx.stub.deleteState.should.have.been.calledOnceWithExactly('1001');
        });

        it('should throw an error for a any log that does not exist', async () => {
            await contract.deleteAnyLog(ctx, '1003').should.be.rejectedWith(/The any log 1003 does not exist/);
        });

    });

});
