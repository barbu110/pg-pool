const genericPool = require('generic-pool');
const util = require('util');
const EventEmitter = require('events').EventEmitter;

class Pool extends EventEmitter {

    constructor(options, Client) {
        super();

        this.options = Object.assign({}, options);
        this.log = this.options.log || (() => {});
        this.Client = this.options.Client || Client || require('pg').Client;
        this.options.max = this.options.max || this.options.poolSize || 10;
        this.options.create = this.options.create || this._create.bind(this);
        this.pool = new genericPool.Pool(this.options);
        this.onCreate = this.options.onCreate;
    }

    _create(callback) {
        this.log('Connecting new client');

        const client = new this.Client(this.options);

        client.on('error', err => {
            this.log('Connected client error: ', err);
            this.pool.destroy(client);

            err.client = client;
            this.emit('error', err);
        });
        client.connect(err => {
            this.log('Client connected');
            this.emit('connect', client);

            if (err) {
                this.log('Connection error: ', err);
                callback(err);
            }

            callback(err, err ? null : client);
        });
    }

    connect(callback) {
        return new Promise((resolve, reject) => {
            this.log('Acquiring client');
            this.pool.acquire((err, client) => {
                if (err) {
                    this.log('Failed acquiring client: ', err);

                    if (callback) {
                        callback(err, null, () => {
                        });
                    }
                    return reject(err);
                }

                this.log('Client acquired');
                this.emit('acquire', client);

                client.release = err => {
                    delete client.release;
                    if (err) {
                        this.log('Failed destroying client: ', err);
                        this.pool.destroy(client);
                    } else {
                        this.log('Client released');
                        this.pool.release(client);
                    }
                };

                if (callback) {
                    callback(null, client, client.release);
                }
                return resolve(client);
            });
        });
    }

    query(text, values, callback) {
        if (typeof values === 'function') {
            callback = values;
            values = undefined;
        }

        return new Promise((resolve, reject) => this.connect((err, client, done) => {
            if (err) {
                return reject(err);
            }

            client.query(text, values, (err, res) => {
                done(err);

                err ? reject(err) : resolve(res);
                if (callback) {
                    callback(err, res);
                }
            });
        }));
    }

    end(callback) {
        this.log('Draining pool');

        return new Promise(resolve => this.pool.drain(() => {
            this.log('Pool drained');

            this.pool.destroyAllNow(() => {
                if (callback) {
                    callback();
                }

                resolve();
            });
        }));
    }

}

module.exports = Pool;
