package cn.nukkit.network;

import cn.nukkit.Player;
import cn.nukkit.Server;
import cn.nukkit.event.player.PlayerCreationEvent;
import cn.nukkit.event.server.QueryRegenerateEvent;
import cn.nukkit.network.protocol.DataPacket;
import cn.nukkit.network.protocol.ProtocolInfo;
import cn.nukkit.network.session.NetherNetPlayerSession;
import cn.nukkit.network.session.NetworkPlayerSession;
import com.google.common.base.Strings;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.cloudburstmc.netty.channel.nethernet.NetherNetChannelFactory;
import org.cloudburstmc.netty.channel.nethernet.NetherNetConstants;
import org.cloudburstmc.netty.channel.nethernet.config.NetherChannelOption;
import org.cloudburstmc.netty.channel.nethernet.signaling.NetherNetHTTPServerSignaling;
import org.cloudburstmc.netty.channel.nethernet.signaling.PongData;
import org.cloudburstmc.netty.util.nethernet.NetherNetLogging;
import org.cloudburstmc.netty.util.nethernet.OperatorIdentity;
import tel.schich.libdatachannel.LibDataChannelArchDetect;
import tel.schich.libdatachannel.PeerConnectionConfiguration;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author MagicDroidX
 * Nukkit Project
 */
@Log4j2
public class NetherNetInterface implements AdvancedSourceInterface {

    private EventLoopGroup eventLoopGroup;
    private Channel netherNetChannel;
    private NetherNetHTTPServerSignaling netherNetSignaling;

    @Getter
    @Setter
    private Network network;
    private final Server server;

    private final Map<InetSocketAddress, NetherNetPlayerSession> sessions = new HashMap<>();
    private final Queue<NetherNetPlayerSession> sessionCreationQueue = PlatformDependent.newMpscQueue();

    public NetherNetInterface(Server server) {
        this.server = server;

        try {
            LibDataChannelArchDetect.initialize();

            NetherNetLogging.setNativeLogLevel("WARN");

            String advertisedAddressesString = server.getPropertyString("advertised-addresses", "");
            Set<String> advertisedAddresses = advertisedAddressesString.isBlank() ? null : new HashSet<>(Arrays.asList(advertisedAddressesString.split(",")));

            NetherNetHTTPServerSignaling.Builder signallingBuilder = new NetherNetHTTPServerSignaling.Builder()
                    .setIceOnLocalPort(false)
                    .setMaxConnectionsPerAddress(server.getPropertyInt("connections-per-address", 8))
                    .setAdvertisedAddresses(advertisedAddresses)
                    .setIdentity(OperatorIdentity.fromPemOrCreate(new File("identity.pem"), "Nukkit"))
                    .setMotdProvider((host, address, client) -> {
                        QueryRegenerateEvent info = this.server.getQueryInformation();

                        return new PongData.Builder()
                                .setServerName(info.getServerName())
                                .setLevelName(info.getWorld())
                                .setProtocol(ProtocolInfo.CURRENT_PROTOCOL)
                                .setVersion(ProtocolInfo.MINECRAFT_VERSION_NETWORK)
                                .setPlayerCount(info.getPlayerCount())
                                .setMaxPlayerCount(info.getMaxPlayerCount())
                                .build();
                    });

            Path httpsConfig = Path.of("keystore.p12");
            boolean https = Files.isRegularFile(httpsConfig);
            if (https) {
                signallingBuilder.setHttpsKeystore(httpsConfig.toFile(), server.getPropertyString("nethernet-keystore-password", ""));
            }

            this.netherNetSignaling = signallingBuilder.build();

            this.eventLoopGroup = new NioEventLoopGroup();

            String address = Strings.isNullOrEmpty(server.getIp()) ? "0.0.0.0" : server.getIp();
            int webrtcPort = server.getPropertyInt("webrtc-port", server.getPort());
            int webrtcMtu = server.getPropertyInt("webrtc-mtu", PeerConnectionConfiguration.AUTO_MTU);

            PeerConnectionConfiguration webrtcConfig = PeerConnectionConfiguration.DEFAULT
                    .withMaxMessageSize(NetherNetConstants.MAX_ADVERTISED_MESSAGE_SIZE)
                    .withBindAddress(InetAddress.getByName(address))
                    .withEnableIceUdpMux(true)
                    .withMtu(webrtcMtu)
                    .withPortRangeBegin((short) webrtcPort)
                    .withPortRangeEnd((short) webrtcPort);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(eventLoopGroup)
                    .channelFactory(NetherNetChannelFactory.server(this.netherNetSignaling))
                    .option(NetherChannelOption.NETHER_PEER_CONNECTION_CONFIG, webrtcConfig)
                    .childHandler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel channel) {
                            NetherNetPlayerSession nukkitSession = new NetherNetPlayerSession(NetherNetInterface.this, channel);
                            channel.pipeline().addLast("nukkit-handler", nukkitSession);
                            NetherNetInterface.this.sessionCreationQueue.offer(nukkitSession);
                        }
                    });

            this.netherNetChannel = bootstrap.bind(new InetSocketAddress(address, server.getPort())).sync().channel();

            server.getLogger().info("NetherNet signaling started on " + (https ? "https://" : "http://") + address + ":" + server.getPort() + " (WebRTC on port " + webrtcPort + "/udp)");
        } catch (Exception e) {
            server.getLogger().error("Failed to start NetherNet", e);
        }
    }

    @Override
    public boolean process() {
        NetherNetPlayerSession session;
        while ((session = this.sessionCreationQueue.poll()) != null) {
            InetSocketAddress address = null;

            try {
                address = (InetSocketAddress) session.getChannel().remoteAddress();

                PlayerCreationEvent event = new PlayerCreationEvent(this, Player.class, Player.class, null, address);
                this.server.getPluginManager().callEvent(event);

                this.sessions.put(event.getSocketAddress(), session);

                Constructor<? extends Player> constructor = event.getPlayerClass().getConstructor(SourceInterface.class, Long.class, InetSocketAddress.class);
                Player player = constructor.newInstance(this, event.getClientId(), event.getSocketAddress());
                session.setPlayer(player);
                this.server.addPlayer(address, player);
            } catch (Exception e) {
                this.server.getLogger().error("Failed to create Player", e);
                session.disconnect("Internal Server Error");
                if (address != null) { // Cast issue
                    this.sessions.remove(address);
                }
            }
        }

        Iterator<NetherNetPlayerSession> iterator = this.sessions.values().iterator();
        while (iterator.hasNext()) {
            NetherNetPlayerSession nukkitSession = iterator.next();
            Player player = nukkitSession.getPlayer();
            if (nukkitSession.getDisconnectReason() != null) {
                player.close(player.getLeaveMessage(), nukkitSession.getDisconnectReason(), false);
                iterator.remove();
            } else {
                nukkitSession.serverTick();
            }
        }
        return true;
    }

    @Override
    public int getNetworkLatency(Player player) {
        NetworkPlayerSession session = player.getNetworkSession();
        return session == null ? -1 : (int) session.getPing();
    }

    @Override
    public NetworkPlayerSession getSession(InetSocketAddress address) {
        return this.sessions.get(address);
    }

    @Override
    public void close(Player player) {
        this.close(player, "unknown reason");
    }

    @Override
    public void close(Player player, String reason) {
        NetworkPlayerSession playerSession = this.getSession(player.getSocketAddress());
        if (playerSession != null) {
            playerSession.disconnect(reason);
        }
    }

    @Override
    public void shutdown() {
        this.sessions.values().forEach(session -> session.disconnect("Shutdown"));
        if (this.netherNetSignaling != null) {
            this.netherNetSignaling.close();
        }
    }

    @Override
    public void emergencyShutdown() {
        this.shutdown();
    }

    @Override
    public void blockAddress(InetSocketAddress address) {

    }

    @Override
    public void blockAddress(InetSocketAddress address, int timeout) {

    }

    @Override
    public void unblockAddress(InetSocketAddress address) {

    }

    @Override
    public void sendRawPacket(InetSocketAddress socketAddress, ByteBuf payload) {
    }

    @Override
    public void setName(String name) {
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet) {
        return this.putPacket(player, packet, false);
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet, boolean needACK) {
        return this.putPacket(player, packet, needACK, false);
    }

    @Override
    public Integer putPacket(Player player, DataPacket packet, boolean needACK, boolean immediate) {
        NetworkPlayerSession session = player.getNetworkSession();
        if (session == null) {
            session = this.sessions.get(player.getSocketAddress());
        }
        if (session != null) {
            session.sendPacket(packet);
        }
        return null;
    }
}
