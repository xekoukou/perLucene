package perLucene;

/*
 Copyright contributors as noted in the AUTHORS file.

 This file is part of PLATANOS.

 PLATANOS is free software; you can redistribute it and/or modify it under
 the terms of the GNU Affero General Public License as published by
 the Free Software Foundation; either version 3 of the License, or
 (at your option) any later version.

 PLATANOS is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;

import java.io.File;

import java.util.List;

//up        1 search
//          2 index + search 

//all replicas should hava 2 as value in case they become leader 
//only the leader can have 1 as value , only the leaders value is checked by the gdd, when it syncs a replica's state

//the existence of up is checked by the leader as well as the gd for all replicas

//the broker object should only be used by the asynchronous thread of the zookeeper

class ZooTiger extends ZooAbstract {

	protected int interval;
	protected int replica;

	protected int leader;
	protected Stat leaderStat = new Stat();

	protected boolean isLeader;
	protected int maximum_size; // only the leader knows these
	protected int split_size;

	protected boolean isSynced = false;

	protected boolean leaderExists;
	protected long lastTimeWorking;

	protected Broker broker;

	// requires a string of 2 size
	ZooTiger() {
		String[] line = Config.readLocalConfig();

		super.initZookeeper(line[0], Integer.parseInt(line[1]), new Watcher() {
			@Override
			public void process(WatchedEvent e) {
				if (!(e.getState()
						.equals(Watcher.Event.KeeperState.SyncConnected))) {
					System.out.println("Disconnected.. exiting");
					System.exit(0);
				}
			}
		});

		interval = Integer.parseInt(line[2]);
		replica = Integer.parseInt(line[3]);

		broker = new Broker();

	}

	public void leader(int leader) {
		this.leader = leader;

	}

	public int leader() {
		return leader;

	}

	public int interval() {
		return interval;

	}

	public boolean isLeader() {

		return isLeader;

	}

	public boolean isSynced() {

		return isSynced;

	}

	public void leaderExists(boolean leaderExists) {

		this.leaderExists = leaderExists;
	}

	public void lastTimeWorking(long lastTimeWorking) {

		this.lastTimeWorking = lastTimeWorking;
	}

	// returns true if the leader is alive
	public boolean getLeader() {
		try {
			String path = "/tiger/Servers/" + Integer.toString(interval);

			leader = ByteBuffer.wrap(
					zoo.getData(path + "/leader", new WatcherLeader(this),
							leaderStat)).getInt();
			// check the leader is alive
			if (zoo.exists(path + "/replicas/" + Integer.toString(leader)
					+ "/up", new WatcherLeaderAlive(this)) != null) {

				if (leader == replica) {
					isLeader = true;
				} else {
					isLeader = false;
				}

				leaderExists = true;
				return true;
			} else {
				this.stoppedWorking();
				return false;
			}

		} catch (KeeperException e) {
			System.out.println("zookeeper client exited with error code "
					+ e.code().toString());
			System.out.println(e.toString());
			System.exit(-1);
		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

		// default value, will never happen
		return false;

	}

	// only done at the beginning, initial value of up is irrelevant
	// goOnline doesnt try to become the Leader since it is assumed that
	// the new replica is unsynced

	// One must manually ask the replica to become leader and it should only be
	// done if all other replicas are down

	public void goOnline(boolean becomeLeader) {

		String path = "/tiger/Servers/" + Integer.toString(interval)
				+ "/replicas/" + Integer.toString(replica) + "/up";

		try {
			zoo.create(path, ByteBuffer.allocate(4).putInt(2).array(), acl,
					CreateMode.EPHEMERAL);

			// any new server is considered unsynched, so unless manually
			// ordered, it never becomes leader

			if (getLeader()) {
				assert (!isLeader);
				sync();
				connect();
			} else {
				if (becomeLeader) {

					becomeLeader();

				}

			}

		} catch (KeeperException e) {
			if (e.code().equals(KeeperException.Code.NODEEXISTS)) {

				System.out
						.println("This replica is already online, possible local configuration error");

			} else {

				System.out.println("zookeeper client exited with error code "
						+ e.code().toString());
				System.out.println(e.toString());

			}
			System.exit(-1);
		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

	}

	// if all replicas are offline, then wait for the last leader to go online
	// this way we dont lose documents

	// all till bind should only be used by the asynchronous thread of zookeeper

	protected void sync() {

	}

	public void connect() {
		broker.connect();

	}

	public void bind() {
		broker.bind();

	}

	public void setMode(boolean index) {
		int value = 1;
		if (index) {
			value = 2;
		}

		try {

			String path = "/tiger/Servers/" + Integer.toString(interval);

			zoo.setData(path + "/replicas" + Integer.toString(replica) + "/up",
					ByteBuffer.allocate(4).putInt(value).array(), -1);

		} catch (KeeperException e) {
			System.out.println("zookeeper client exited with error code "
					+ e.code().toString());
			System.out.println(e.toString());
			System.exit(-1);
		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

	}

	public void becomeLeader() {

		isLeader = true;

		try {
			String path = "/tiger/Servers/" + Integer.toString(interval);

			zoo.setData(path + "/leader", ByteBuffer.allocate(4)
					.putInt(replica).array(), leaderStat.getVersion());

		} catch (KeeperException e) {
			if (e.code().equals(KeeperException.Code.BADVERSION)) {

				isLeader = false;
				// someone else became a leader
			} else {
				// TODO maybe clarify the possible errors
				System.out.println("zookeeper client exited with error code "
						+ e.code().toString());
				System.out.println(e.toString());
				System.exit(-1);
			}
		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

		if (isLeader) {

			String path = "/tiger/Servers/" + Integer.toString(interval);

			try {

				maximum_size = ByteBuffer.wrap(
						zoo.getData(path + "/maximum_size", new WatcherNotify(
								this), null)).getInt();

				split_size = ByteBuffer.wrap(
						zoo.getData(path + "/split_size", new WatcherNotify(
								this), null)).getInt();

			} catch (KeeperException e) {
				System.out.println("zookeeper client exited with error code "
						+ e.code().toString());
				System.out.println(e.toString());
				System.exit(-1);
			} catch (Exception e) {
				System.out.println("zookeeper client interrupted");
				System.out.println(e.toString());
				System.exit(-1);
			}

			// get which other replicas are online

			findReplicas();

			// inform the IndexThread to bind
			bind();

		}

	}

	// findReplicas should only be executed one at a time

	public void findReplicas() {

		String path = "/tiger/Servers/" + Integer.toString(interval);

		try {
			List<String> children = zoo.getChildren(path + "/replicas",
					new WatcherReplicas(this));

			List<Integer> aliveReplicas = new ArrayList<Integer>();

			for (int i = 0; i < children.size(); i++) {

				if (zoo.exists(path + "/replicas/" + children.get(i) + "/up",
						new WatcherReplicaAlive(this)) != null) {
					aliveReplicas.add(Integer.parseInt(children.get(i)));

				}

			}

			// update the indexerThread
			broker.updateReplicas(aliveReplicas);

		} catch (KeeperException e) {
			System.out.println("zookeeper client exited with error code "
					+ e.code().toString());
			System.out.println(e.toString());
			System.exit(-1);
		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

	}

	public void stoppedWorking() {

		if (leaderExists) {
			this.leaderExists(false);
			this.lastTimeWorking((new Date()).getTime());
		}

	}

	public String getConfig() {

		try {
			Stat stat = new Stat();

			return (new String(zoo.getData("/tiger/Servers/"
					+ Integer.toString(interval) + "/replicas/"
					+ Integer.toString(replica), null, stat), "UTF-8"));

		} catch (KeeperException e) {

			System.out
					.println("This replica doesnt exists, or its configuration in zookeeper is corrupt");
			System.exit(0);

		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

		return null;

	}

	// used only when this replica is the leader

	// we assume that each replica has the same index
	// we assume that the index is in its own filesystem

	// 1 need to split notification
	// 2 indexing stopped notification

	public void notifyZoo() {

		assert (isLeader);

		try {
			File file = new File("/mnt/perLucene");

			int free = (int) (file.getFreeSpace() / 1024 ^ 3);
			int total = (int) (file.getTotalSpace() / 1024 ^ 3);
			int used = total - free;

			if (used > split_size) {

				String path = "/tiger/Servers/" + Integer.toString(interval);

				if (used > maximum_size) {
					// stop indexing until the problem is fixed

					zoo.setData(path + "/" + Integer.toString(replica) + "/up",
							ByteBuffer.allocate(4).putInt(1).array(), -1);

					zoo.setData(path + "/notifications", ByteBuffer.allocate(4)
							.putInt(2).array(), -1);

				} else {

					zoo.setData(path + "/notifications", ByteBuffer.allocate(4)
							.putInt(1).array(), -1);

				}

			}

		} catch (KeeperException e) {

			System.out.println("zookeeper client exited with error code "
					+ e.code().toString());
			System.out.println(e.toString());
			System.exit(-1);

		} catch (Exception e) {
			System.out.println("zookeeper client interrupted");
			System.out.println(e.toString());
			System.exit(-1);
		}

	}

}
