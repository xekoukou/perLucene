package perLucene;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

class WatcherReplicaAlive implements Watcher {

	protected ZooTiger tiger;

	WatcherReplicaAlive(ZooTiger tiger) {
		this.tiger = tiger;

	}

	public void process(WatchedEvent e) {

		// TODO update the IndexerThread

	}
}
