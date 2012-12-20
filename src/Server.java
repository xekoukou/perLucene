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

import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.document.LongDocValuesField;
import org.apache.lucene.index.DirectoryReader;


import java.io.IOException;
import java.io.File;
import java.util.HashMap;
import java.lang.System;

public class Server
{

    private static DirectoryReader r;
    private static IndexWriter w;
//conatins all the analyzers that are to be used by IndexWriter or the query
    private static HashMap < String, Analyzer > hAnalyzer;

    public static void main ()
    {

        GreekAnalyzer analyzer = new GreekAnalyzer (Version.LUCENE_40);

          initAnalyzers ();
          initIndexWriter ();
          initIndexReader ();
    }

    private static void initAnalyzers ()
    {

        hAnalyzer.put ("ar", new ArabicAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("el", new GreekAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("bg", new BulgarianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("br", new BrazilianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("ca", new CatalanAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("cz", new CzechAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("da", new DanishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("de", new GermanAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("en", new EnglishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("es", new SpanishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("eu", new BasqueAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("fa", new PersianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("fi", new FinnishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("fr", new FrenchAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("ga", new IrishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("gl", new GalicianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("hi", new HindiAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("hu", new HungarianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("hy", new ArmenianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("id", new IndonesianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("it", new ItalianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("lv", new LatvianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("nl", new DutchAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("no", new NorwegianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("pt", new PortugueseAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("ro", new RomanianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("ru", new RussianAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("sv", new SwedishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("th", new ThaiAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("tr", new TurkishAnalyzer (Version.LUCENE_40));
        hAnalyzer.put ("cn", new SmartChineseAnalyzer (Version.LUCENE_40));

    }

    private static void initIndexWriter ()
    {

        try {
            Directory dir = new MMapDirectory (new File ("/mnt/index"));

            //FSDirectory.setMaxMergeWriteMBPerSec. 
            //use this if merges interfere with the search

            //use a unix native directory to tell the os not to cache merges

            NRTCachingDirectory index =
                new NRTCachingDirectory (dir, 20, 400.0);


            IndexWriterConfig config =
                new IndexWriterConfig (Version.LUCENE_40, null);
            //used to reduce lattency of obtaining a reader after a big merge
            // config.setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer());

            config.setMergePolicy (new LogByteSizeMergePolicy ());

            w = new IndexWriter (index, config);
        } catch (IOException e) {
            System.out.println ("Stacktrace " + e.toString ());
            System.exit (0);
        }
    }

    private static void addDoc (Analyzer analyzer, String language,
                                String summary, String text,
                                long uid) throws IOException
    {
        Document doc = new Document ();
          doc.add (new TextField ("summary", summary, Field.Store.NO));
          doc.add (new TextField ("text", text, Field.Store.NO));
          doc.add (new LongDocValuesField ("uid", uid));
          doc.add (new StringField ("language", language, Field.Store.NO));

          w.addDocument (doc, analyzer);
    }

//must be called only if the writer is initialized
    private static void initIndexReader ()
    {
        try {
            r = DirectoryReader.open (w, true);
        } catch (IOException e) {
            System.out.println ("Stacktrace " + e.toString ());
            System.exit (0);
        }

    }

    private static void updateIndexReader ()
    {
        try {
            DirectoryReader tr = DirectoryReader.openIfChanged (r, w, true);
            if (tr != null) {
                r.close ();
                r = tr;
            }
        }
        catch (IOException e) {
            System.out.println ("Stacktrace " + e.toString ());
            System.exit (0);
        }

    }



}
