package perLucene;

import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.PollItem;
import org.jeromq.ZMQ.Socket;

class Stalk {

	private ZContext ctx;
	private Socket ssearch;
	private Socket ssres;

	Stalk(String location) {
		ctx = new ZContext();
		// bindings for search
		// in fact it should be push-pull
		ssearch = ctx.createSocket(ZMQ.DEALER);

		String address = "tcp://" + location + ":49000";
		ssearch.bind(address);

		ssres = ctx.createSocket(ZMQ.ROUTER);

		address = "tcp://" + location + ":49001";
		ssres.bind(address);

	}

	private void search(ZMsg msg) {

		// TODO
	}

	public void poll() {

		PollItem[] items = new PollItem[] { new PollItem(ssearch, ZMQ.POLLIN) };
		while (true) {
			ZMQ.poll(items, -1);
			if (items[0].isReadable()) {

				ZMsg msg = ZMsg.recvMsg(ssearch);
				search(msg);
			}
		}

	}

}
