package perLucene;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

class WatcherNotify implements Watcher {

	protected ZooTiger tiger;

	WatcherNotify(ZooTiger tiger) {
		this.tiger = tiger;

	}

	public void process(WatchedEvent e) {

		if (tiger.isLeader()) {
			tiger.notifyZoo();

		}

	}

}
