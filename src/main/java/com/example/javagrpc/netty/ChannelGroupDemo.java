package com.example.javagrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

/**
 * 用 {@link EmbeddedChannel} 示範 {@link ChannelGroup} 如何集中管理多個連線，
 * 並對其中一部分連線做「選擇性廣播」。
 *
 * <p>直接執行本類別的 {@code main} 方法即可在 console 觀察結果，不需要真的開 TCP 連線。
 *
 * <h2>為什麼要練習這個技術</h2>
 * 聊天室、遊戲伺服器、訂閱推播（行情、通知）等場景，伺服器同時持有大量連線，
 * 常見需求是「把同一則訊息送給多個（甚至全部）連線」。如果每次都自己維護一個
 * {@code List<Channel>}，需要處理：
 * <ul>
 *   <li>多執行緒下對這個集合的並發新增/移除/遍歷安全性。</li>
 *   <li>連線斷線時要記得從集合中移除，否則會持有已關閉 channel 的殘留參考。</li>
 *   <li>「廣播」時要逐一呼叫 {@code writeAndFlush}，並正確處理每個 channel 各自的
 *       {@link io.netty.channel.ChannelFuture}。</li>
 * </ul>
 *
 * <h2>不可取代性</h2>
 * <ul>
 *   <li><b>執行緒安全</b>：{@link DefaultChannelGroup} 內部用執行緒安全的集合實作，
 *       可以在任意執行緒呼叫 {@code add}/{@code remove}/{@code writeAndFlush}，
 *       不需要自己加鎖。</li>
 *   <li><b>自動清理</b>：{@code add(channel)} 時會替該 channel 的
 *       {@link Channel#closeFuture()} 註冊 listener，連線關閉時自動把它從群組中移除，
 *       不會有「殭屍參考」殘留在集合裡。</li>
 *   <li><b>群組廣播即一行程式碼</b>：{@code group.writeAndFlush(msg)} 會對群組內每個
 *       channel 各自呼叫一次 {@code writeAndFlush}，並回傳一個
 *       {@link io.netty.channel.group.ChannelGroupFuture} 可以統一等待/監聽整體完成。</li>
 * </ul>
 *
 * <h2>適用場景</h2>
 * <ul>
 *   <li>聊天室 / IM：把訊息廣播給同一個房間（room）裡的所有連線。</li>
 *   <li>遊戲伺服器：廣播場景事件給同一張地圖/同一房間的所有玩家連線。</li>
 *   <li>行情推播、系統公告：對所有目前在線的連線廣播。</li>
 * </ul>
 * <p>不適用：一對一的單一連線通訊（例如本專案的 gRPC unary RPC），
 * 這種情況直接持有單一 {@code Channel}/{@code StreamObserver} 即可，不需要群組管理。
 */
public class ChannelGroupDemo {

    /** 用 {@link AttributeKey} 標記每個連線所屬的聊天室，作為選擇性廣播的依據。 */
    private static final AttributeKey<String> ROOM = AttributeKey.valueOf("room");

    private ChannelGroupDemo() {
    }

    public static void main(String[] args) {
        demoBroadcastToAll();
        System.out.println();
        demoSelectiveBroadcastByRoom();
        System.out.println();
        demoGroupAutoRemovesClosedChannel();
    }

    /** 把同一則訊息廣播給群組內所有連線。 */
    private static void demoBroadcastToAll() {
        System.out.println("=== 廣播給所有連線 ===");
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel alice = newChannel();
        EmbeddedChannel bob = newChannel();
        EmbeddedChannel carol = newChannel();
        group.add(alice);
        group.add(bob);
        group.add(carol);

        group.writeAndFlush("系統公告：伺服器將於 5 分鐘後進行維護\n");

        printOutbound(alice, "Alice");
        printOutbound(bob, "Bob");
        printOutbound(carol, "Carol");
    }

    /** 只把訊息廣播給特定聊天室（room-1）裡的連線，room-2 的連線不會收到。 */
    private static void demoSelectiveBroadcastByRoom() {
        System.out.println("=== 選擇性廣播（只送給 room-1） ===");
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel alice = newChannel();
        EmbeddedChannel bob = newChannel();
        EmbeddedChannel carol = newChannel();
        alice.attr(ROOM).set("room-1");
        bob.attr(ROOM).set("room-1");
        carol.attr(ROOM).set("room-2");

        group.add(alice);
        group.add(bob);
        group.add(carol);

        broadcastToRoom(group, "room-1", "room-1 訊息：歡迎加入聊天室！\n");

        printOutbound(alice, "Alice(room-1)");
        printOutbound(bob, "Bob(room-1)");
        printOutboundOrNull(carol, "Carol(room-2)");
    }

    /** 連線關閉後，{@link ChannelGroup} 會自動把它從群組中移除。 */
    private static void demoGroupAutoRemovesClosedChannel() {
        System.out.println("=== 連線關閉後自動從 ChannelGroup 移除 ===");
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel alice = newChannel();
        EmbeddedChannel bob = newChannel();
        group.add(alice);
        group.add(bob);
        System.out.println("加入兩個連線後，group.size() = " + group.size());

        alice.close();
        System.out.println("Alice 關閉連線後，group.size() = " + group.size());
    }

    /** 遍歷群組，只對 {@link #ROOM} 屬性符合 roomId 的連線送出訊息。 */
    private static void broadcastToRoom(ChannelGroup group, String roomId, String message) {
        for (Channel channel : group) {
            if (roomId.equals(channel.attr(ROOM).get())) {
                channel.writeAndFlush(message);
            }
        }
    }

    /**
     * 建立一個帶有唯一 {@link ChannelId} 的 {@link EmbeddedChannel}。
     *
     * <p>{@link EmbeddedChannel} 預設都共用同一個 {@code EmbeddedChannelId.INSTANCE}，
     * 若不指定唯一 ID，{@link DefaultChannelGroup}（以 {@link ChannelId} 為 key）
     * 會把後續加入的連線視為重複而忽略，群組裡永遠只會剩下第一個連線。
     */
    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(DefaultChannelId.newInstance(), new StringEncoder(CharsetUtil.UTF_8));
    }

    private static void printOutbound(EmbeddedChannel channel, String name) {
        ByteBuf buf = channel.readOutbound();
        String received = buf.toString(CharsetUtil.UTF_8).trim();
        buf.release();
        System.out.println(name + " 收到：" + received);
    }

    private static void printOutboundOrNull(EmbeddedChannel channel, String name) {
        ByteBuf buf = channel.readOutbound();
        if (buf == null) {
            System.out.println(name + " 沒有收到任何訊息");
        } else {
            System.out.println(name + " 收到：" + buf.toString(CharsetUtil.UTF_8).trim());
            buf.release();
        }
    }
}
