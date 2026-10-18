package io.github.centrifugal.centrifuge;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;


/**
 * The fetch of a redirect of an {@link RpcPool}: the URL check of the endpoint, the request, and the body read within
 * its bound. The client follows no redirect, and a 3xx answer fails the fetch.
 */
final class RpcPoolRedirect {

    // The largest initial buffer of a body: the bytes are buffered as they arrive, after at most this many up front.
    private static final int INITIAL_BUFFER_MAX = 1024 * 1024;

    // The initial buffer of a body whose length is not declared.
    private static final int INITIAL_BUFFER_UNDECLARED = 8 * 1024;

    private RpcPoolRedirect() {
    }


    /**
     * Whether the redirect URL may carry the data of the endpoint. A secure endpoint, {@code wss:} or {@code https:}, needs an
     * https URL; a plain one takes https or http.
     */
    static boolean allowed(String endpoint, String url) {
        String lowerUrl = url.toLowerCase(Locale.ROOT);
        String lowerEndpoint = endpoint.toLowerCase(Locale.ROOT);
        if (lowerEndpoint.startsWith("wss:") || lowerEndpoint.startsWith("https:")) {
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
     * The body of a successful response, read up to the limit. A redirect answer is not successful, so it fails here. The
     * bytes are buffered as they arrive: the buffer starts at most at the declared length, and at most at 1 MiB, and grows
     * up to the limit. The response is closed by the caller, which releases the body on every outcome.
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
        long declared = body.contentLength();
        if (declared > maxBytes) {
            throw new IOException("redirect body is over " + maxBytes + " bytes");
        }
        // The buffer starts within the declared length, the initial bound and the limit, and grows up to the limit.
        long start = declared >= 0 ? Math.min(declared, INITIAL_BUFFER_MAX) : INITIAL_BUFFER_UNDECLARED;
        byte[] buffer = new byte[(int) Math.max(1L, Math.min(start, maxBytes))];
        int length = 0;
        InputStream in = body.byteStream();
        while (true) {
            if (length == buffer.length) {
                if (length >= maxBytes) {
                    // The buffer holds the limit: one more byte is over it, and none is the end of the body.
                    if (in.read() == -1) {
                        return Arrays.copyOf(buffer, length);
                    }
                    throw new IOException("redirect body is over " + maxBytes + " bytes");
                }
                long grown = Math.min((long) buffer.length * 2L, maxBytes);
                buffer = Arrays.copyOf(buffer, (int) grown);
            }
            int read = in.read(buffer, length, buffer.length - length);
            if (read == -1) {
                if (declared >= 0 && length < declared) {
                    throw new IOException("redirect body ended before its declared length");
                }
                return Arrays.copyOf(buffer, length);
            }
            length += read;
        }
    }
}
