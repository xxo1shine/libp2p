package org.tron.p2p.discover.protocol.kad2.business.call;

import java.net.InetSocketAddress;
import org.tron.p2p.discover.message.Message;

public interface CallBack {

  void handleResponse(Message message, InetSocketAddress address);

  void timeout(InetSocketAddress address);
}
