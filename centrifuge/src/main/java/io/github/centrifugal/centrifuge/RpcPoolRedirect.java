package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;


/**
 * The fetch of a redirect of an {@link RpcPool}: the URL check of the endpoint, the request, and the
 * body read within its bound.
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
     * Starts the GET of the redirect. The callback gets the response, or the failure of the call.
     *
     * @throws IllegalArgumentException when the URL or a header is not valid.
     */
    static okhttp3.Call enqueue(OkHttpClient client, RpcPool.Redirect redirect, Callback callback) {
        Request.Builder builder = new Request.Builder().url(redirect.getUrl());
        for (Map.Entry<String, String> header : redirect.getHeaders().entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }
        okhttp3.Call call = client.newCall(builder.build());
        call.enqueue(callback);
        return call;
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
