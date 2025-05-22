package org.tron.p2p.discover.protocol.kad2.business.call;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad2.RequestMessage;

@Data
public class Call {
  private boolean requested;
  private final RequestMessage message;
  private final MessageType responseType;
  private final ScheduledExecutorService timer;
  private final CallBack callBack;

  public Call(RequestMessage message, MessageType responseType, CallBack callBack) {
    this.message = message;
    this.responseType = responseType;
    this.callBack = callBack;
    this.timer = Executors.newSingleThreadScheduledExecutor(
            new BasicThreadFactory.Builder().namingPattern("callTimer").build());
  }
}
