/**
 * The netty server can be configured via a JSON file specified via `-Dserver.config=file.json`. By default, the server
 * starts up on all interfaces with plaintext port 8080 (port 443 when SSL is enabled), a token expiration duration of 5
 * minutes, a scheduler pool size of 4, and a max inbound message size of 100 MiB. This is represented by the following
 * JSON:
 *
 * <pre>
 * {
 *   "host": "0.0.0.0",
 *   "port": 8080,
 *   "tokenExpire": "PT5m",
 *   "schedulerPoolSize": 4,
 *   "maxInboundMessageSize": 104857600
 * }
 * </pre>
 *
 * To bind to the local interface instead of all interfaces:
 *
 * <pre>
 * {
 *   "host": "127.0.0.1"
 * }
 * </pre>
 *
 * To enable SSL, you can can add an `ssl` and `identity` section:
 *
 * <pre>
 * {
 *   "ssl": {
 *     "identity": {
 *       "type": "privatekey",
 *       "certChainPath": "ca.crt",
 *       "privateKeyPath": "key.pem"
 *     }
 *   }
 * }
 * </pre>
 *
 * If your identity material is in the Java keystore format, you can specify that with the `keystore` identity type:
 *
 * <pre>
 * {
 *   "ssl": {
 *     "identity": {
 *       "type": "keystore",
 *       "path": "keystore.p12",
 *       "password": "super-secret"
 *     }
 *   }
 * }
 * </pre>
 *
 * The SSL block provides further options for configuring SSL behavior:
 *
 * <pre>
 * {
 *   "ssl": {
 *     "protocols": ["TLSv1.3"],
 *     "ciphers": ["TLS_AES_128_GCM_SHA256"]
 *   }
 * }
 * </pre>
 *
 * @see io.deephaven.server.netty.NettyMain
 * @see io.deephaven.server.netty.NettyConfig
 */
package io.deephaven.server.netty;
