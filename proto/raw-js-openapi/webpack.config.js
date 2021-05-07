const path = require('path');

module.exports = {
    mode: "production",
    output: {
        path: __dirname+'/build/js-out/',
        filename: 'dh-internal.js',
        libraryTarget: "umd",
    },
    resolve : {
        modules: ['node_modules', __dirname + '/build/js-src'],
    },
};