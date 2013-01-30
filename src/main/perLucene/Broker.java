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
import java.util.List;

//Commands
// 1 bind
// 2 connect
// 3 updateReplicas

class Broker {

	protected Socket sbroker;

	protected ZContext ctx;

	PollItem items[] = new PollItem[1];

	Broker() {
		ctx = new ZContext();

		sbroker = ctx.createSocket(ZMQ.DEALER);

		String address = "tcp://127.0.0.1:49002";
		sbroker.bind(address);

		items[0] = new PollItem(sbroker, ZMQ.POLLIN);

	}

	// lazy pirate

	public void bind() {
		while (true) {
			ZMsg msg = new ZMsg();

			msg.add(new ZFrame(ByteBuffer.allocate(4).putInt(1).array()));
			msg.send(sbroker);
			ZMQ.poll(items, 1000);
			if (items[0].isReadable()) {

				ZMsg resp = ZMsg.recvMsg(sbroker);
				ZFrame frame = resp.pop();
				int command = ByteBuffer.wrap(frame.getData()).getInt();
				frame.destroy();
				resp.destroy();
				assert (command == 1);
				break;
			}
		}
	}

	public void connect() {
		while (true) {
			ZMsg msg = new ZMsg();
			msg.add(new ZFrame(ByteBuffer.allocate(4).putInt(2).array()));
			msg.send(sbroker);
			ZMQ.poll(items, 1000);
			if (items[0].isReadable()) {

				ZMsg resp = ZMsg.recvMsg(sbroker);
				ZFrame frame = resp.pop();
				int command = ByteBuffer.wrap(frame.getData()).getInt();
				frame.destroy();
				resp.destroy();
				assert (command == 2);
				break;
			}
		}
	}

	public void updateReplicas(List<Integer> children) {

		while (true) {
			ZMsg msg = new ZMsg();
			msg.add(new ZFrame(ByteBuffer.allocate(4).putInt(3).array()));

			for (int i = 0; i < children.size(); i++) {
				msg.add(new ZFrame(ByteBuffer.allocate(4).putInt(
						children.get(i)).array()));
			}

			msg.send(sbroker);
			ZMQ.poll(items, 1000);
			if (items[0].isReadable()) {

				ZMsg resp = ZMsg.recvMsg(sbroker);
				ZFrame frame = resp.pop();
				int command = ByteBuffer.wrap(frame.getData()).getInt();
				frame.destroy();
				resp.destroy();
				assert (command == 3);
				break;
			}
		}

	}

}
