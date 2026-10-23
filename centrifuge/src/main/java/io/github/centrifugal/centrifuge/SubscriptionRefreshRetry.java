package io.github.centrifugal.centrifuge;

/**
 * The retry policy of one subscription's token refresh: the failures in a row set the step of the next delay, and the
 * token the server refused with 109 is remembered so that it is not sent again while stopOnRefusedToken is set.
 * The delay itself is the connection's refresh retry ({@link Client#refreshRetryDelayMillis(int)}).
 */
final class SubscriptionRefreshRetry {

    /** What a failed refresh does to the subscription. */
    enum Verdict {
        RETRY,
        KEEP,
        UNSUBSCRIBE
    }

    private final Client client;
    // Consecutive failed token refreshes of this subscription: the step of its next retry delay.
    private int step = 0;
    // The token the server refused with 109 in the last refresh; null when none was refused since the last success.
    private String refusedToken;

    SubscriptionRefreshRetry(final Client client) {
        this.client = client;
    }

    /** True when the fresh token is the one the server refused and it must not be sent again. */
    boolean isRefused(final String token) {
        return client.getOpts().isStopOnRefusedToken() && token.equals(refusedToken);
    }

    /** Classifies a refusal of a refresh sent with the token. The same classification as the connection refresh. */
    Verdict onRefusal(final ReplyError error, final String token) {
        if (AuthRefresh.isRetryableRefresh(error)) {
            if (error.getCode() == AuthRefresh.ERROR_CODE_TOKEN_EXPIRED) {
                refusedToken = token;
            }
            return Verdict.RETRY;
        }
        if (error.getCode() == AuthRefresh.PERMISSION_DENIED) {
            return Verdict.KEEP;
        }
        return Verdict.UNSUBSCRIBE;
    }

    /** A subscribe succeeded: the next refresh failure starts from the first step again. */
    void resetStep() {
        step = 0;
    }

    /** A refresh was accepted: the step starts again and the refused token is forgotten. */
    void accepted() {
        step = 0;
        refusedToken = null;
    }

    /** The delay of the next retry, in milliseconds; the step moves on for the retry after it. */
    long nextDelayMillis() {
        long delayMillis = client.refreshRetryDelayMillis(step);
        step = Math.min(step + 1, AuthRefresh.RETRY_MAX_STEP);
        return delayMillis;
    }
}
