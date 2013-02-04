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

import com.sun.net.httpserver.Authenticator.Success;

import java.nio.ByteBuffer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import java.util.Iterator;

import java.util.TreeMap;

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

		// id to response
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
		 * msg frames 1 gdid 2+ (other docfields)
		 * */
		protected void index(ZMsg msg) {
			ZFrame address = msg.unwrap();

			// since we delete docs if the msg is 1, this is to protect the
			// database from malevolence
			if (msg.size() < 2) {
				msg.destroy();
				return;
			}

			ZFrame gdid = msg.first().duplicate();

			long id = ithread.incrementId();

			// create response and store

			ZMsg resp = new ZMsg();
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(id).array()));
			resp.add(gdid);
			resp.wrap(address);
			hResps.put(id, resp);

			// send to all replicas and index yourself
			Iterator<ZFrame> it = hIds.keySet().iterator();

			while (it.hasNext()) {
				ZMsg mrepl = msg.duplicate();
				mrepl.addFirst(new ZFrame(ByteBuffer.allocate(8).putLong(id)
						.array()));
				mrepl.wrap(it.next().duplicate());
				mrepl.send(sindex);
			}

			if (addDoc(msg, id)) {
			} else {
				failed(id);
			}

		}

		// 1 means it failed
		// 0 means it succeeded

		// a msg with only the gdid means the deletion of that gdid
		protected void failed(long id) {
			// send failed msg to originator
			ZMsg msg = hResps.get(id);

			// maybe it has already been asked to be deleted

			if (msg != null) {
				hResps.remove(id);
				msg.add(ByteBuffer.allocate(4).putInt(1).array());
				msg.send(sgdb);
			}
			// send deletes to replicas anyway because the msg might not exist
			// because this is a new leader
			// ofcourse this creates duplicate deletes to be sent
			ZMsg resp = new ZMsg();
			resp
					.addFirst(new ZFrame(ByteBuffer.allocate(8).putLong(id)
							.array()));

			Iterator<ZFrame> it = hIds.keySet().iterator();

			while (it.hasNext()) {

				ZMsg dresp = resp.duplicate();
				dresp.wrap(it.next().duplicate());
				dresp.send(sindex);
			}
			resp.destroy();

		}

		protected void failed(long start, long end) {

			// TODO need some small optimization
			for (long it = start; it <= end; it++) {
				// send failed msg to originator
				ZMsg msg = hResps.get(it);

				// maybe it has already been asked to be deleted

				if (msg != null) {
					hResps.remove(it);
					msg.add(ByteBuffer.allocate(4).putInt(1).array());
					msg.send(sgdb);
				}
			}

			// send deletes to replicas anyway because the msg might not exist
			// because this is a new leader
			// ofcourse this creates duplicate deletes to be sent
			ZMsg resp = new ZMsg();
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(end).array()));
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(start).array()));

			Iterator<ZFrame> it = hIds.keySet().iterator();

			while (it.hasNext()) {

				ZMsg dresp = resp.duplicate();
				dresp.wrap(it.next().duplicate());
				dresp.send(sindex);
			}
			resp.destroy();

		}

		protected void success(ZFrame address, long id) {
			// send success msg to originator
			ZMsg msg = hResps.get(id);

			hResps.remove(id);
			msg.add(ByteBuffer.allocate(4).putInt(0).array());
			msg.send(sgdb);

		}

		protected void getResp(ZMsg msg) {

			ZFrame address = msg.unwrap();

			Iterator<ZFrame> it = msg.iterator();

			long end = ByteBuffer.wrap(it.next().data()).getLong();
			long start;

			int failed = ByteBuffer.wrap(it.next().data()).getInt();

			if (failed == 1) {

				// in case this is a new Leader that has lost some messages
				// update id if necessary
				if (end > ithread.id) {
					ithread.id = end;
				}

				if (it.hasNext()) {
					start = ByteBuffer.wrap(it.next().data()).getLong();
					failed(start, end);
					ithread.deleteDocs(start, end);
				} else {
					failed(end);
					ithread.deleteDoc(end);
				}
				address.destroy();
			} else {
				success(address, end);
			}

		}

	}

	private class Replica {

		Replica() {
			PollItem temp = items[0];
			items = new PollItem[3];
			items[0] = temp;
		}

		protected void index(ZMsg msg) {
			if (msg.size() < 3) {
				deleteDocs(msg);
			} else {
				ZFrame frame = msg.pop();
				long id = ByteBuffer.wrap(frame.data()).getLong();
				frame.destroy();

				long localId = ithread.incrementId();

				if (id > localId) {
					multDelResp(localId + 1, id - 1);
				}
				if (id >= localId) {

					if (addDoc(msg, id)) {
						sendResp(id, 0);
					} else {
						sendResp(id, 1);
					}
				} else {
					multDelResp(id, localId);
				}

			}
		}

		// id
		// gdid
		// 0 success 1 failure
		protected void sendResp(long id, int failed) {

			ZMsg resp = new ZMsg();
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(id).array()));

			resp
					.add(new ZFrame(ByteBuffer.allocate(4).putLong(failed)
							.array()));

			resp.send(sindex);

		}

		protected void multDelResp(long start, long end) {
			ZMsg resp = new ZMsg();
			resp.addFirst(new ZFrame(ByteBuffer.allocate(8).putLong(end)
					.array()));
			resp.add(new ZFrame(ByteBuffer.allocate(4).putLong(1).array()));
			resp.add(new ZFrame(ByteBuffer.allocate(8).putLong(start).array()));

			resp.send(sindex);
		}

	}

	// gdid
	// summary
	// text
	// wkt
	// language
	protected boolean addDoc(ZMsg msg, long id) {

		Iterator<ZFrame> it = msg.iterator();
		try {
			byte[] gdid = it.next().data();
			String sum = new String(it.next().data(), "UTF-8");
			String text = new String(it.next().data(), "UTF-8");
			String wkt = new String(it.next().data(), "UTF-8");
			String language = new String(it.next().data(), "UTF-8");

			long date = DateProducer.date();

			msg.destroy();

			return ithread.addDoc(language, sum, text, date, wkt, gdid, id);
		} catch (Exception e) {
		}
		return false;
	}

	protected void deleteDocs(ZMsg msg) {
		Iterator<ZFrame> it = msg.iterator();

		long end = ByteBuffer.wrap(it.next().data()).getLong();
		long start;
		if (it.hasNext()) {
			start = ByteBuffer.wrap(it.next().data()).getLong();
			ithread.deleteDocs(start, end);
		} else {

			ithread.deleteDoc(end);
		}
		msg.destroy();
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

				if (items[1].isReadable()) {

					ZMsg msg = ZMsg.recvMsg(sindex);
					((Leader) server).getResp(msg);
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
