package org.tron.p2p.discover.protocol.kad2.table;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.tron.p2p.discover.protocol.kad2.table.Kad2Options.BUCKET_SIZE;
import static org.tron.p2p.discover.protocol.kad2.table.Kad2Options.MAX_REPLACEMENTS;

@Slf4j(topic = "net")
public class NodeBucket {
  private final Table table;
  @Getter
  private final Set<NodeEntry> entries = new HashSet<>();
  @Getter
  private final Queue<NodeEntry> replacements = new LinkedList<>();

  public NodeBucket(Table table) {
    this.table = table;
  }

  public void add(NodeEntry entry) {
    if (entries.size() < BUCKET_SIZE) {
      entries.add(entry);
      entry.setInTable(true);
      table.getListeners().forEach(listener -> listener.nodeAdded(entry.getNode()));
    } else {
      replacements.add(entry);
      if (replacements.size() > MAX_REPLACEMENTS) {
        table.dropNode(entry.getNode());
      }
    }
  }

  public void remove(NodeEntry entry) {
    if (!entries.contains(entry)) {
      replacements.remove(entry);
      return;
    }

    entries.remove(entry);
    table.getListeners().forEach(listener -> listener.nodeRemoved(entry.getNode()));
    if (!replacements.isEmpty()) {
      NodeEntry e = replacements.poll();
      entries.add(e);
      e.setInTable(true);
      table.getListeners().forEach(listener -> listener.nodeAdded(e.getNode()));
    }
  }
}
