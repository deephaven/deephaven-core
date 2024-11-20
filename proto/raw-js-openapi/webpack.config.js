const path = require('path');

// Workaround for broken codegen from protoc-gen-js using import_style=commonjs_strict, both in
// the grpc-web protoc-gen-ts plugin, and in protoc-gen-js itself:
const aliases = {};
for (const proto of ['application', 'config', 'console', 'hierarchicaltable', 'inputtable', 'object', 'partitionedtable', 'session', 'storage', 'table', 'ticket']) {
    // Allows a reference to the real proto files, to be made from the shim
    aliases[`real/${proto}_pb`] =  `${__dirname}/build/js-src/deephaven_core/proto/${proto}_pb`;

    const shimPath = `${__dirname}/src/shim/${proto}_pb`;
    // Three aliases which would normally point at the real proto file, now directed to the shim:
    // * First, an unsuffixed, relative reference from any service files
    aliases[`../../deephaven_core/proto/${proto}_pb`] = shimPath;
    // * Next, a ".js"-suffixed, relative reference from other proto files (see https://github.com/protocolbuffers/protobuf-javascript/issues/40)
    aliases[`../../deephaven_core/proto/${proto}_pb.js`] = shimPath;
    // * Last, an absolute reference from the index.js
    aliases[`deephaven_core/proto/${proto}_pb`] = shimPath;
}
for (const proto of ['Flight', 'BrowserFlight']) {
    // Allows a reference to the real proto files, to be made from the shim
    aliases[`real/${proto}_pb`] =  `${__dirname}/build/js-src/${proto}_pb`;

    const shimPath = `${__dirname}/src/shim/${proto}_pb`;
    // Two aliases which would normally point to the real proto file, now directed to the shim:
    // * First, a relative reference from any service file
    aliases[`./${proto}_pb`] = shimPath;
    // * Second, an absolute reference from the index.js
    aliases[`${proto}_pb`] = shimPath;
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