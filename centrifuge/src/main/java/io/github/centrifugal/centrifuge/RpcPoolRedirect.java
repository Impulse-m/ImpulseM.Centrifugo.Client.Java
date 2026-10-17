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
 * The fetch of a redirect of an {@link RpcPool}: the URL check of the endpoint, the request, and the body read within
 * its bound. The client follows no redirect, and a 3xx answer fails the fetch.
 */
final class RpcPoolRedirect {

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
     * The GET of the redirect URL, with the request headers of the redirect.
     *
     * @throws IllegalArgumentException when a header is not valid.
     */
    static Request request(HttpUrl url, RpcPool.Redirect redirect) {
        Request.Builder builder = new Request.Builder().url(url);
        for (Map.Entry<String, String> header : redirect.getHeaders().entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }
        return builder.build();
    }


    /**
     * The body of a successful response, read up to the limit. A redirect answer is not successful, so it fails here.
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
