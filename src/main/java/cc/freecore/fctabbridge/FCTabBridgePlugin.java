package cc.freecore.fctabbridge;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.ProxyServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import me.neznamy.tab.api.TabAPI;
import me.neznamy.tab.api.TabPlayer;
import org.slf4j.Logger;

@Plugin(id = "fctabbridge", name = "FCTabBridge", version = "1.0.0")
public final class FCTabBridgePlugin {
    private static final Pattern REDIS_URL = Pattern.compile("(?m)^\\s*url:\\s*['\\\"]?(redis://[^'\\\"\\s]+)");
    private final ProxyServer proxy;
    private final Logger logger;
    private final Map<UUID, TabName> names = new ConcurrentHashMap<>();
    private final Map<UUID, String> appliedNames = new ConcurrentHashMap<>();
    private Thread listenerThread;

    @Inject
    public FCTabBridgePlugin(ProxyServer proxy, Logger logger) {
        this.proxy = proxy;
        this.logger = logger;
    }

    @Subscribe
    public void onInitialize(ProxyInitializeEvent event) {
        logger.info("FCTabBridge initialized; subscribing to fctransfer:tab-name");
        listenerThread = new Thread(this::listen, "FCTabBridge-Redis");
        listenerThread.setDaemon(true);
        listenerThread.start();
        proxy.getScheduler().buildTask(this, this::applyNames).repeat(1, TimeUnit.SECONDS).schedule();
    }

    private void listen() {
        while (!proxy.isShuttingDown()) {
            try {
                RedisEndpoint endpoint = readEndpoint();
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(endpoint.host, endpoint.port), 2000);
                    socket.setSoTimeout(0);
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    if (!endpoint.password.isEmpty() && send(output, input, "AUTH", endpoint.password) == null) continue;
                    if (endpoint.database != 0 && send(output, input, "SELECT", String.valueOf(endpoint.database)) == null) continue;
                    subscribe(output, input, "fctransfer:tab-name");
                    logger.info("FCTabBridge subscribed to fctransfer:tab-name");
                    while (!proxy.isShuttingDown()) {
                        String[] message = readArray(input);
                        if (message.length != 3 || !"message".equals(message[0])) continue;
                        String[] parts = message[2].split("\\|", 3);
                        if (parts.length == 2) {
                            names.put(UUID.fromString(parts[0]), new TabName(null, new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8)));
                        } else if (parts.length == 3) {
                            names.put(UUID.fromString(parts[0]), new TabName(parts[1], new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8)));
                        }
                        logger.debug("Received tab name update for {}", parts.length == 3 ? parts[1] : parts[0]);
                    }
                }
            } catch (Exception exception) {
                logger.warn("FCTabBridge Redis listener retry: {}", exception.toString());
                try { Thread.sleep(1000L); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private void applyNames() {
        TabAPI api = TabAPI.getInstance();
        if (api == null) return;
        names.forEach((uuid, tabName) -> {
            TabPlayer player = api.getPlayer(uuid);
            if (player == null && tabName.playerName != null) player = api.getPlayer(tabName.playerName);
            if (player != null && player.isLoaded()) {
                // TAB accepts legacy ampersand formatting. Keep a trailing color code in
                // the same string as the player name so it colors the name as intended.
                String formattedName = tabName.displayName.replace('\u00A7', '&');
                api.getTabListFormatManager().setName(player, formattedName);
                if (!formattedName.equals(appliedNames.put(uuid, formattedName))) {
                    logger.info("Applied bridged TAB name for {}", player.getName());
                }
            } else if (tabName.playerName != null) {
                appliedNames.remove(uuid);
                logger.debug("TAB player {} is not loaded yet", tabName.playerName);
            }
        });
    }

    private RedisEndpoint readEndpoint() throws Exception {
        String config = Files.readString(Path.of("C:\\MCSManager\\daemon\\data\\InstanceData\\FreeCore-Proxy\\plugins\\TAB\\config.yml"));
        Matcher matcher = REDIS_URL.matcher(config);
        if (!matcher.find()) throw new IOException("TAB Redis URL is missing.");
        URI uri = new URI(matcher.group(1));
        String userInfo = uri.getUserInfo();
        String password = "";
        if (userInfo != null) {
            int separator = userInfo.indexOf(':');
            password = separator >= 0 ? userInfo.substring(separator + 1) : userInfo;
        }
        String path = uri.getPath();
        int database = path == null || path.length() < 2 ? 0 : Integer.parseInt(path.substring(1));
        return new RedisEndpoint(uri.getHost(), uri.getPort() < 0 ? 6379 : uri.getPort(), database, password);
    }

    private String send(OutputStream output, InputStream input, String... values) throws IOException {
        writeCommand(output, values);
        int type = input.read();
        if (type == -1 || type == '-') { readLine(input); return null; }
        String line = readLine(input);
        if (type == '$') {
            int length = Integer.parseInt(line);
            if (length < 0) return null;
            byte[] data = input.readNBytes(length);
            input.read(); input.read();
            return new String(data, StandardCharsets.UTF_8);
        }
        return line;
    }

    private void subscribe(OutputStream output, InputStream input, String channel) throws IOException {
        writeCommand(output, "SUBSCRIBE", channel);
        String[] response = readArray(input);
        if (response.length != 3 || !"subscribe".equals(response[0]) || !channel.equals(response[1])) {
            throw new IOException("Unexpected Redis subscription response.");
        }
    }

    private void writeCommand(OutputStream output, String... values) throws IOException {
        StringBuilder request = new StringBuilder("*").append(values.length).append("\r\n");
        for (String value : values) request.append("$").append(value.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(value).append("\r\n");
        output.write(request.toString().getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    private String[] readArray(InputStream input) throws IOException {
        if (input.read() != '*') throw new IOException("Expected Redis array.");
        int count = Integer.parseInt(readLine(input));
        String[] values = new String[count];
        for (int index = 0; index < count; index++) {
            int type = input.read();
            if (type == '$') {
                int length = Integer.parseInt(readLine(input));
                if (length < 0) {
                    values[index] = null;
                    continue;
                }
                byte[] data = input.readNBytes(length);
                if (input.read() != '\r' || input.read() != '\n') throw new IOException("Malformed Redis bulk value.");
                values[index] = new String(data, StandardCharsets.UTF_8);
            } else if (type == '+' || type == ':') {
                values[index] = readLine(input);
            } else if (type == '-') {
                throw new IOException("Redis error: " + readLine(input));
            } else {
                throw new IOException("Unsupported Redis array value.");
            }
        }
        return values;
    }

    private String readLine(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int next;
        while ((next = input.read()) != -1 && next != '\r') bytes.write(next);
        if (next == -1 || input.read() != '\n') throw new IOException("Malformed Redis response.");
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private record RedisEndpoint(String host, int port, int database, String password) { }

    private record TabName(String playerName, String displayName) { }
}
