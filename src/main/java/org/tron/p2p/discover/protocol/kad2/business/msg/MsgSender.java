package org.tron.p2p.discover.protocol.kad2.business.msg;

import java.net.InetSocketAddress;
import java.util.function.Consumer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.socket.UdpEvent;

@Slf4j(topic = "net")
public class MsgSender {
  @Setter
  private static Consumer<UdpEvent> messageSender;

  public static void sendMessage(Message msg, InetSocketAddress address) {
    if (messageSender != null) {
      messageSender.accept(new UdpEvent(msg, address));
    }
  }
}
