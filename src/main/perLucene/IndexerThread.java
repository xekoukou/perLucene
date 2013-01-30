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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

import java.util.HashMap;
import java.io.IOException;
import java.util.HashSet;

//Each index replica sends the last id<->gdid that it has indexed
//when it commits

class IndexerThread implements Runnable {

	protected IndexWriter w;
	protected HashMap<String, Analyzer> ha;

	protected Italk italk;

	protected String location;
	protected int replica;

	protected long id; // ids are incremented in big-endian unsigned integer
						// order

	IndexerThread(IndexWriter w, HashMap<String, Analyzer> ha, String location,
			int replica, long id) {
		this.w = w;
		this.ha = ha;
		this.location = location;
		this.replica = replica;
		this.id = id;
	}

	// ids are incremented in big-endian unsigned integer order
	public long incrementId() {
		Long i = id;
		long mask = 1;

		while ((i & mask) != 0) {
			i &= ~mask;
			mask <<= 1;
		}
		i |= mask;

		id = i;
		return id;
	}

	// Add spatial search support
	public void addDoc(String language, String summary, String text, long date)
			throws IOException {
		Analyzer analyzer = ha.get(language);

		Document doc = new Document();
		doc.add(new TextField("summary", summary, Field.Store.NO));
		doc.add(new TextField("text", text, Field.Store.NO));
		doc.add(new LongDocValuesField("uid", id));
		doc.add(new StringField("language", language, Field.Store.NO));
		doc.add(new LongField("date", date, Field.Store.NO));
		w.addDocument(doc, analyzer);
	}

	private void softSync() {

	}

	private void Sync() {

	}

	private void commit() {

		try {
			w.commit();
		} catch (IOException io) {
			System.out.println("Error,while trying to commit..");
			try {
				if (w != null) {
					w.close();
				}
			} catch (IOException e) {
			}

			finally {
				System.exit(-1);
			}
		}
	}

	@Override
	public void run() {
		italk = new Italk(this, location, replica);
		italk.poll();

	}

}
