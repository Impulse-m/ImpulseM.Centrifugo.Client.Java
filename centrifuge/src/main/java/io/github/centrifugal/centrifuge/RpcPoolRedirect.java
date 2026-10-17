package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;


/**
 * The fetch of a redirect of an {@link RpcPool}: the URL checks of the endpoint and of each hop, the request of each
 * hop, and the body read within its bound. The pool follows the hops itself, so the application headers reach only
 * the host the redirect names.
 */
final class RpcPoolRedirect {
    /** Redirects a fetch follows after its first request. The response after the last one fails the fetch. */
    static final int MAX_HOPS = 5;

    private RpcPoolRedirect() {
    }


    /**
     * Whether the redirect URL may carry the data of the endpoint. A secure endpoint needs an https URL,
     * a plain one takes https or http.
     */
    static boolean allowed(String endpoint, String url) {
        String lowerUrl = url.toLowerCase(Locale.ROOT);
        if (endpoint.toLowerCase(Locale.ROOT).startsWith("wss:")) {
            return lowerUrl.startsWith("https:");
        }
        return lowerUrl.startsWith("https:") || lowerUrl.startsWith("http:");
    }


    /**
     * Whether a hop may follow the URL it answers. The endpoint rule of {@link #allowed} applies to every hop, and an
     * https URL never hands over to a plain http one, whatever the endpoint is.
     */
    static boolean hopAllowed(String endpoint, HttpUrl from, HttpUrl to) {
        if (from.isHttps() && !to.isHttps()) {
            return false;
        }
        return allowed(endpoint, to.toString());
    }


    /** Whether the response is a redirect the pool follows: a 301, 302, 303, 307 or 308. */
    static boolean isRedirect(Response response) {
        int code = response.code();
        return code == 301 || code == 302 || code == 303 || code == 307 || code == 308;
    }


    /**
     * The URL a redirect response points to, resolved against the URL it answers.
     *
     * @throws IOException when the response has no valid location, or the hop is not allowed for the endpoint.
     */
    static HttpUrl nextUrl(Response response, String endpoint) throws IOException {
        HttpUrl from = response.request().url();
        String location = response.header("Location");
        HttpUrl next = location != null ? from.resolve(location) : null;
        if (next == null) {
            throw new IOException("redirect has no valid location");
        }
        if (!hopAllowed(endpoint, from, next)) {
            throw new IOException("redirect url is not allowed for the endpoint");
        }
        return next;
    }


    /**
     * The GET of one hop. The request headers of the redirect go only to the host and port of the URL the hook named,
     * the allowed URL: a hop to any other host is requested without them.
     *
     * @throws IllegalArgumentException when a header is not valid.
     */
    static Request request(HttpUrl url, HttpUrl allowedUrl, RpcPool.Redirect redirect) {
        Request.Builder builder = new Request.Builder().url(url);
        if (url.host().equals(allowedUrl.host()) && url.port() == allowedUrl.port()) {
            for (Map.Entry<String, String> header : redirect.getHeaders().entrySet()) {
                builder.header(header.getKey(), header.getValue());
            }
        }
        return builder.build();
    }


    /**
     * The body of a successful response, read up to the limit.
     *
     * @throws IOException when the status is not 2xx, the body is over the limit, or it cannot be read.
     */
    static byte[] readBody(Response response, long maxBytes) throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException("redirect answered " + response.code());
        }
        ResponseBody body = response.body();
        if (body == null) {
            return new byte[0];
        }
        if (body.contentLength() > maxBytes) {
            throw new IOException("redirect body is over " + maxBytes + " bytes");
        }
        BufferedSource source = body.source();
        // Reads one byte past the limit at most, so that a body without a length is bounded too.
        source.request(maxBytes + 1);
        if (source.getBuffer().size() > maxBytes) {
            throw new IOException("redirect body is over " + maxBytes + " bytes");
        }
        return source.readByteArray();
    }
}
