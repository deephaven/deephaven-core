const path = require('path');
const aliases = {};
for (var proto of ['application', 'config', 'console', 'hierarchicaltable', 'inputtable', 'object', 'partitionedtable', 'session', 'storage', 'table', 'ticket']) {
    aliases[`../../deephaven/proto/${proto}_pb`] = `${__dirname}/src/shim/deephaven/proto/${proto}_pb`;
    aliases[`../../deephaven/proto/${proto}_pb.js`] = `${__dirname}/src/shim/deephaven/proto/${proto}_pb`;
    aliases[`deephaven/proto/${proto}_pb`] = `${__dirname}/src/shim/deephaven/proto/${proto}_pb`;
    aliases[`real/${proto}_pb`] =  `${__dirname}/build/js-src/deephaven/proto/${proto}_pb`;
}
for (var proto of ['Flight', 'BrowserFlight']) {
    aliases[`./${proto}_pb`] = `${__dirname}/src/shim/${proto}_pb`;
    aliases[`${proto}_pb`] = `${__dirname}/src/shim/${proto}_pb`;
    aliases[`real/${proto}_pb`] =  `${__dirname}/build/js-src/${proto}_pb`;
}

module.exports = {
    mode: "production",
    module: {
        rules: [{ test: /\.ts?$/, use: 'ts-loader', exclude: /node_modules/ }]
    },
    output: {
        path: __dirname+'/build/js-out/',
        filename: 'dh-internal.js',
        libraryTarget: 'module',
        module:true,
    },
    experiments: {
        outputModule:true
    },
    resolve : {
        modules: ['node_modules', __dirname + '/build/js-src'],
        extensions: ['.ts', '.js'],
        alias: aliases,
    },
};