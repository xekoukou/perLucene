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

import org.jeromq.ZMQ;
import org.jeromq.ZContext;
import org.jeromq.ZFrame;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.PollItem;
import org.jeromq.ZMQ.Socket;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import java.nio.ByteBuffer;

class Italk {

	protected ZContext ctx;

	protected Socket sbroker;
	protected Socket sindex;
	protected Socket scommands;
	protected Socket ssync;

	protected int replica; // used to set the id of the dealer socket

	PollItem items[];

	IndexerThread ithread;

	private String location;

	private Object server;

	Italk(IndexerThread ithread, String location, int replica) {

		this.location = location;
		this.replica = replica;

		this.ithread = ithread;
		ctx = new ZContext();

		sbroker = ctx.createSocket(ZMQ.ROUTER);

		String address = "tcp://127.0.0.1:49002";
		sbroker.connect(address);

		items[0] = new PollItem(sbroker, ZMQ.POLLIN);

	}

	// lazy pirate

	protected void sbroker_talk(ZMsg msg) {

		ZFrame address = msg.unwrap();
		ZFrame frame;

		ZMsg resp = new ZMsg();

		frame = msg.pop();

		int command = ByteBuffer.wrap(frame.getData()).getInt();
		if (command == 1) {
			bind();

			resp.add(frame);
		}

		if (command == 2) {
			connect();

			resp.add(frame);
		}
		if (command == 3) {
			assert (server instanceof Leader);
			((Leader) server).updateReplicas(msg);

			resp.add(frame);
		}

		msg.destroy();
		resp.wrap(address);
		resp.send(sbroker);
	}

	public void bind() {

		if (sindex == null) {
			sindex.close();
		}
		sindex = ctx.createSocket(ZMQ.ROUTER);

		String address = "tcp://" + location + ":49004";

		sindex.bind(address);

		if (scommands == null) {
			scommands.close();
		}

		scommands = ctx.createSocket(ZMQ.ROUTER);

		address = "tcp://" + location + ":49003";

		scommands.bind(address);

		server = new Leader();

		items[1] = new PollItem(sindex, ZMQ.POLLIN);
		items[2] = new PollItem(scommands, ZMQ.POLLIN);

	}

	public void connect() {

		if (sindex == null) {
			sindex.close();
		}
		sindex = ctx.createSocket(ZMQ.DEALER);

		sindex.setIdentity(ByteBuffer.allocate(4).putInt(replica).array());

		String address = "tcp://" + location + ":49004";

		sindex.connect(address);

		if (scommands == null) {
			scommands.close();
		}
		scommands = ctx.createSocket(ZMQ.DEALER);

		scommands.setIdentity(ByteBuffer.allocate(4).putInt(replica).array());

		address = "tcp://" + location + ":49003";

		scommands.connect(address);

		server = new Replica();

		items[1] = new PollItem(sindex, ZMQ.POLLIN);
		items[2] = new PollItem(scommands, ZMQ.POLLIN);

	}

	private class Leader {

		protected Socket sgdb;

		// address to Ids
		protected HashMap<ZFrame, Long> hIds; // the ids of the replicas used
		// by the router
		// when this replica is the leader

		// Ids to number of addresses with same id
		protected TreeMap<Long, Integer> tIds;
		protected Long min;

		protected HashMap<Long, ZMsg> hResps;

		Leader() {

			PollItem temp = items[0];
			items = new PollItem[4];
			items[0] = temp;

			sgdb = ctx.createSocket(ZMQ.ROUTER);
			String address = "tcp://" + location + ":49005";
			sgdb.setIdentity(ByteBuffer.allocate(4).putInt(replica).array());

			sgdb.bind(address);
			items[3] = new PollItem(sgdb, ZMQ.POLLIN);

		}

		// ids should be bigger than 0
		protected void updateReplicas(ZMsg msg) {

			hIds = new HashMap<ZFrame, Long>();
			tIds = new TreeMap<Long, Integer>(new Comparator<Long>() {

				public int compare(Long first, Long second) {
					long diff = first ^ second;
					if (diff != 0) {
						long mask = 0x8000000000000000L;
						if ((diff ^ mask) == 0) {
							if (first > second) {
								return 1;
							} else {
								return 0;
							}
						} else {
							if ((first ^ mask) != 0) {
								return 1;
							} else {
								return 0;
							}
						}

					} else {
						return 0;
					}
				}
			});

			Iterator<ZFrame> it = msg.descendingIterator();
			while (it.hasNext()) {
				ZFrame frame = it.next();
				hIds.put(frame, 0L);

			}

		}

		/**
		 * msg frames 1 gdid 
		 * 2+ (other docfields)
		 * */
		protected void index(ZMsg msg) {
			ZFrame address = msg.unwrap();
			ZFrame gdid = msg.first().duplicate();
			long id = ithread.incrementId();

			// create response and store

			ZMsg resp = new ZMsg();
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(id).array()));
			resp.add(gdid);
			resp.wrap(address);
			hResps.put(id, resp);

			// send to all replicas and index yourself
			Iterator<ZFrame> it=hIds.keySet().iterator();
			
			while(it.hasNext()){
				ZMsg mrepl=msg.duplicate();
				mrepl.wrap(it.next().duplicate());
				mrepl.send(sindex);
			}
			
			addDoc(msg);
			

		}

	}

	private class Replica {

		Replica() {
			PollItem temp = items[0];
			items = new PollItem[3];
			items[0] = temp;
		}

		protected void index(ZMsg msg) {
        addDoc(msg);
 		}

	}
	
	protected void addDoc(ZMsg msg){
		
	}

	public void poll() {
		// duplicating code here

		if (server instanceof Leader) {
			while (true) {
				ZMQ.poll(items, 10);
				if (items[0].isReadable()) {

					ZMsg msg = ZMsg.recvMsg(sbroker);
					sbroker_talk(msg);
				}
				if (items[3].isReadable()) {
					ZMsg msg = ZMsg.recvMsg(((Leader) server).sgdb);
					((Leader) server).index(msg);
				}

			}
		} else {

			while (true) {
				ZMQ.poll(items, 10);
				if (items[0].isReadable()) {

					ZMsg msg = ZMsg.recvMsg(sbroker);
					sbroker_talk(msg);
				}
				if (items[1].isReadable()) {

					ZMsg msg = ZMsg.recvMsg(sindex);
					((Replica) server).index(msg);
				}

			}

		}

	}

}
