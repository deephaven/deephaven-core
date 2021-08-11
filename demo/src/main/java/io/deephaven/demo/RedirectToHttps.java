package io.deephaven.demo;

/**
 * RedirectToHttps:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 20/07/2021 @ 1:02 a.m..
 */
public class RedirectToHttps {
    /*

    Found a set of kotlin stack overflows which give good groundwork inspiration for an https redirect in vert.x:

    https://stackoverflow.com/questions/39564570/in-vertx-i-need-to-redirect-all-http-requests-to-the-same-url-but-for-https
    https://stackoverflow.com/questions/39564199/i-have-a-vertx-request-and-i-need-to-calculate-an-externally-visible-public-ur/39564200#39564200

    // return current URL as public URL
fun RoutingContext.externalizeUrl(): String {
    return externalizeUrl(URI(request().absoluteURI()).pathPlusParmsOfUrl())
}

// resolve a related URL as a public URL
fun RoutingContext.externalizeUrl(resolveUrl: String): String {
    val cleanHeaders = request().headers().filterNot { it.value.isNullOrBlank() }
            .map { it.key to it.value }.toMap()
    return externalizeURI(URI(request().absoluteURI()), resolveUrl, cleanHeaders).toString()
}

internal fun externalizeURI(requestUri: URI, resolveUrl: String, headers: Map<String, String>): URI {
    // special case of not touching fully qualified resolve URL's
    if (resolveUrl.startsWith("http://") || resolveUrl.startsWith("https://")) return URI(resolveUrl)

    val forwardedScheme = headers.get("X-Forwarded-Proto")
            ?: headers.get("X-Forwarded-Scheme")
            ?: requestUri.getScheme()

    // special case of //host/something URL's
    if (resolveUrl.startsWith("//")) return URI("$forwardedScheme:$resolveUrl")

    val (forwardedHost, forwardedHostOptionalPort) =
            dividePort(headers.get("X-Forwarded-Host") ?: requestUri.getHost())

    val fallbackPort = requestUri.getPort().let { explicitPort ->
        if (explicitPort <= 0) {
            if ("https" == forwardedScheme) 443 else 80
        } else {
            explicitPort
        }
    }
    val requestPort: Int = headers.get("X-Forwarded-Port")?.toInt()
            ?: forwardedHostOptionalPort
            ?: fallbackPort
    val finalPort = when {
        forwardedScheme == "https" && requestPort == 443 -> ""
        forwardedScheme == "http" && requestPort == 80 -> ""
        else -> ":$requestPort"
    }

    val restOfUrl = requestUri.pathPlusParmsOfUrl()
    return URI("$forwardedScheme://$forwardedHost$finalPort$restOfUrl").resolve(resolveUrl)
}


internal fun URI.pathPlusParmsOfUrl(): String {
    val path = this.getRawPath().let { if (it.isNullOrBlank()) "" else it.mustStartWith('/') }
    val query = this.getRawQuery().let { if (it.isNullOrBlank()) "" else it.mustStartWith('?') }
    val fragment = this.getRawFragment().let { if (it.isNullOrBlank()) "" else it.mustStartWith('#') }
    return "$path$query$fragment"
}

internal fun dividePort(hostWithOptionalPort: String): Pair<String, Int?> {
    val parts = if (hostWithOptionalPort.startsWith('[')) { // ipv6
        Pair(hostWithOptionalPort.substringBefore(']') + ']', hostWithOptionalPort.substringAfter("]:", ""))
    } else { // ipv4
        Pair(hostWithOptionalPort.substringBefore(':'), hostWithOptionalPort.substringAfter(':', ""))
    }
    return Pair(parts.first, if (parts.second.isNullOrBlank()) null else parts.second.toInt())
}

fun String.mustStartWith(prefix: Char): String {
    return if (this.startsWith(prefix)) { this } else { prefix + this }
}




fun Route.redirectToHttpsHandler(publicHttpsPort: Int = 443, redirectCode: Int = 302, failOnUrlBuilding: Boolean = true) {
    handler { context ->
        val proto = context.request().getHeader("X-Forwarded-Proto")
                ?: context.request().getHeader("X-Forwarded-Scheme")
        if (proto == "https") {
            context.next()
        } else if (proto.isNullOrBlank() && context.request().isSSL) {
            context.next()
        } else {
            try {
                val myPublicUri = URI(context.externalizeUrl())
                val myHttpsPublicUri = URI("https",
                        myPublicUri.userInfo,
                        myPublicUri.host,
                        publicHttpsPort,
                        myPublicUri.rawPath,
                        myPublicUri.rawQuery,
                        myPublicUri.rawFragment)
                context.response().putHeader("location", myHttpsPublicUri.toString()).setStatusCode(redirectCode).end()
            } catch (ex: Throwable) {
                if (failOnUrlBuilding) context.fail(ex)
                else context.next()
            }
        }
    }
}

OR

fun Route.simplifiedRedirectToHttpsHandler(publicHttpsPort: Int = 443, redirectCode: Int = 302, failOnUrlBuilding: Boolean = true) {
    handler { context ->
        try {
            val myPublicUri = URI(context.externalizeUrl())
            if (myPublicUri.scheme == "http") {
                val myHttpsPublicUri = URI("https",
                        myPublicUri.userInfo,
                        myPublicUri.host,
                        publicHttpsPort,
                        myPublicUri.rawPath,
                        myPublicUri.rawQuery,
                        myPublicUri.rawFragment)
                context.response().putHeader("location", myHttpsPublicUri.toString()).setStatusCode(redirectCode).end()
            }
            else {
                context.next()
            }
        } catch (ex: Throwable) {
            if (failOnUrlBuilding) context.fail(ex)
            else context.next()
        }
    }
}






    */
}
