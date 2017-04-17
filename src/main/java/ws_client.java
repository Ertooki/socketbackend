import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import org.json.simple.JSONObject;

@ClientEndpoint
public class ws_client {
    Session userSession = null;
    String name;
    private MessageHandler messageHandler;

    public ws_client(String n) {
        name = n;
    }

    @OnOpen
    public void onOpen(Session userSession) {
        main.infoLogger.info("opening "+name+" websocket");
        this.userSession = userSession;
        JSONObject sess = new JSONObject();
        sess.put("command", "request_session");
        JSONObject params_sess = new JSONObject();
        params_sess.put("site_id", new Integer(358));
        params_sess.put("language", "ger");
        sess.put("params", params_sess);
        sendMessage(sess.toString());
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        System.out.println("closing websocket " + name+ " "+reason.getReasonPhrase()+ " " + reason.getCloseCode());
        this.userSession = null;
    }

    /**
     * Callback hook for Message Events. This method will be invoked when a client send a message.
     *
     * @param message The text message
     */
    @OnMessage
    public void onMessage(String message) {
        if (this.messageHandler != null) {
            this.messageHandler.handleMessage(message);
        }
    }

    /**
     * register message handler
     *
     * @param msgHandler
     */
    public void addMessageHandler(MessageHandler msgHandler) {
        this.messageHandler = msgHandler;
    }

    /**
     * Send a message.
     *
     * @param message
     */
    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

    /**
     * Message handler.
     *
     * @author Jiji_Sasidharan
     */
    public static interface MessageHandler {

        public void handleMessage(String message);
    }
}