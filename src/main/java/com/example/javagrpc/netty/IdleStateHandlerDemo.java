package com.example.javagrpc.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * 用 {@link EmbeddedChannel} 示範 {@link IdleStateHandler} 如何偵測「心跳逾時」並主動斷線。
 *
 * <p>直接執行本類別的 {@code main} 方法即可在 console 觀察結果，不需要真的開 TCP 連線；
 * 透過 {@link EmbeddedChannel#advanceTimeBy} + {@link EmbeddedChannel#runScheduledPendingTasks()}
 * 模擬時間經過，觸發 {@link IdleStateHandler} 內部排程的逾時檢查。
 *
 * <h2>為什麼要練習這個技術</h2>
 * TCP 連線斷線（對方斷電、網路中斷、process 被 kill）時，作業系統不一定會立刻通知對方，
 * 連線可能會變成「半開（half-open）」：本端以為連線還活著，但對方早已消失。
 * 長連線服務（IM、遊戲伺服器、MQ、RPC 長連線）若不主動偵測，會持續持有這些殭屍連線，
 * 浪費 file descriptor 與記憶體，甚至讓訊息送進黑洞。
 *
 * <p>解法是約定「心跳機制」：客戶端定期送心跳封包，伺服器若在指定時間內都沒有收到
 * 任何資料（含心跳），就視為對方已經失聯，主動關閉連線釋放資源。
 *
 * <h2>不可取代性</h2>
 * <ul>
 *   <li><b>正確性</b>：「多久沒收到資料就算逾時」需要精準的計時與排程，
 *       自己用 {@code Thread.sleep} 或額外的 timer thread 維護容易出現競態條件
 *       （連線關閉的同時又收到資料）。{@link IdleStateHandler} 是 Netty 的標準實作，
 *       與 EventLoop 排程整合，不需要額外執行緒。</li>
 *   <li><b>一行設定取代重複樣板</b>：只要把
 *       {@code new IdleStateHandler(readerIdleSeconds, writerIdleSeconds, allIdleSeconds, unit)}
 *       加入 pipeline，就能同時偵測「太久沒收到資料」、「太久沒送出資料」、
 *       「雙向都太久沒有 I/O」三種情境，並透過 {@link IdleStateEvent} 通知後續 handler。</li>
 *   <li><b>關注點分離</b>：{@link IdleStateHandler} 只負責「偵測並發出事件」，
 *       「逾時後要做什麼（關閉連線、送心跳包、記錄 log）」交給後續 handler 的
 *       {@code userEventTriggered}，職責清楚、容易測試。</li>
 * </ul>
 *
 * <h2>適用場景</h2>
 * <ul>
 *   <li>長連線協議的伺服器/客戶端（IM、遊戲伺服器、MQTT/IoT、自訂 TCP 協議）。</li>
 *   <li>需要定期送心跳維持連線的場景：{@code WRITER_IDLE} 觸發時送出心跳包，
 *       避免中間的 NAT/防火牆因連線閒置而切斷。</li>
 *   <li>需要偵測對方失聯並釋放資源的場景：{@code READER_IDLE} 觸發時關閉連線。</li>
 * </ul>
 * <p>不適用：短連線（一次請求一次回應後即關閉）的協議（如一般 HTTP API），
 * 連線生命週期已經由請求/回應週期決定，不需要額外的閒置偵測。
 */
public class IdleStateHandlerDemo {

    /** 超過這個秒數沒有收到任何資料，就視為心跳逾時。 */
    private static final int READER_IDLE_SECONDS = 3;

    private IdleStateHandlerDemo() {
    }

    public static void main(String[] args) {
        demoHeartbeatKeepsAlive();
        System.out.println();
        demoIdleTimeoutClosesChannel();
    }

    /** 每隔 2 秒送一次心跳（小於 3 秒的逾時門檻），連線應該持續存活。 */
    private static void demoHeartbeatKeepsAlive() {
        System.out.println("=== 持續送心跳（每 2 秒一次），連線維持 ===");
        EmbeddedChannel channel = newChannel();

        for (int i = 1; i <= 3; i++) {
            channel.advanceTimeBy(2, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            channel.writeInbound("HEARTBEAT");
            channel.readInbound();
            System.out.println("第 " + i + " 次：經過 2 秒並收到心跳，連線狀態 open=" + channel.isOpen());
        }
    }

    /** 完全不送任何資料，超過 3 秒後應觸發 READER_IDLE 並主動關閉連線。 */
    private static void demoIdleTimeoutClosesChannel() {
        System.out.println("=== 完全沒有心跳，逾時後主動斷線 ===");
        EmbeddedChannel channel = newChannel();

        channel.advanceTimeBy(READER_IDLE_SECONDS - 1, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        System.out.println("經過 " + (READER_IDLE_SECONDS - 1) + " 秒，尚未逾時，連線狀態 open=" + channel.isOpen());

        channel.advanceTimeBy(2, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        System.out.println("再經過 2 秒（累計超過 " + READER_IDLE_SECONDS
                + " 秒沒有讀取事件）後，連線狀態 open=" + channel.isOpen());
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                // readerIdle=3 秒、writerIdle/allIdle 不啟用（傳 0 表示不檢查）。
                new IdleStateHandler(READER_IDLE_SECONDS, 0, 0, TimeUnit.SECONDS),
                new HeartbeatTimeoutHandler());
    }

    /** 收到 {@link IdleState#READER_IDLE} 事件時，視為對方心跳逾時，主動關閉連線。 */
    private static class HeartbeatTimeoutHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent idleEvent && idleEvent.state() == IdleState.READER_IDLE) {
                System.out.println(">> " + READER_IDLE_SECONDS + " 秒內未收到任何資料（心跳逾時），主動關閉連線");
                ctx.close();
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }
}
