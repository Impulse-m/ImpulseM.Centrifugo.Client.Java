package io.github.centrifugal.centrifuge;


/**
 * The failure of {@link TokenExchangeEvent#rpc} whose server error has an app type listed in
 * {@link TokenExchange#refusalTypes}: the issuer refused the refresh token. An {@link UnauthorizedException},
 * so an {@link TokenExchange#exchange} which fails with it, or rethrows it, starts the refusal run.
 */
public final class TokenExchangeRefusedException extends UnauthorizedException {
    private final ReplyError replyError;


    TokenExchangeRefusedException(ReplyError replyError) {
        this.replyError = replyError;
    }


    /** The server error the rpc call was rejected with: its code, message, app code, app type and metadata. */
    public ReplyError getReplyError() {
        return replyError;
    }
}
