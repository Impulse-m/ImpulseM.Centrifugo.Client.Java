package io.github.centrifugal.centrifuge;

public class ConnectedEvent {
    public String getClient() {
        return client;
    }

    void setClient(String client) {
        this.client = client;
    }

    public byte[] getData() {
        return data;
    }

    void setData(byte[] data) {
        this.data = data;
    }

    /** User the connection is authenticated as. Empty when the server reports no user. */
    public String getUser() {
        return user;
    }

    void setUser(String user) {
        this.user = user;
    }

    /** Authentication class of the connection. Empty when the server reports no class. */
    public String getAuthClass() {
        return authClass;
    }

    void setAuthClass(String authClass) {
        this.authClass = authClass;
    }

    private byte[] data;

    private String client;

    private String user;

    private String authClass;
}
