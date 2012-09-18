/* ********************************************************************
    Licensed to Jasig under one or more contributor license
    agreements. See the NOTICE file distributed with this work
    for additional information regarding copyright ownership.
    Jasig licenses this file to you under the Apache License,
    Version 2.0 (the "License"); you may not use this file
    except in compliance with the License. You may obtain a
    copy of the License at:
        
    http://www.apache.org/licenses/LICENSE-2.0
        
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied. See the License for the
    specific language governing permissions and limitations
    under the License.
*/
package org.bedework.calsvc.indexing;

import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.exc.CalFacadeException;

import edu.rpi.cct.misc.indexing.Index;
import edu.rpi.cct.misc.indexing.IndexException;
import edu.rpi.cct.misc.indexing.SearchLimits;
import edu.rpi.sss.util.xml.XmlEmit;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.io.StringWriter;

import javax.xml.namespace.QName;

/**
 * @author Mike Douglass douglm - rpi.edu
 *
 */
public class BwIndexSolrImpl implements BwIndexer {
  private BwIndexKey keyConverter = new BwIndexKey();

  private int batchMaxSize = 0;
  private int batchCurSize = 0;
  private XmlEmit batch;

  private static final QName solrTagAdd = new QName(null, "add");
//  private static final QName solrTagDoc = new QName(null, "doc");
//  private static final QName solrTagField = new QName(null, "field");
//  private static final QName solrTagOptimize = new QName(null, "optimize");

  /* Used to batch index */

  /** Constructor
   *
   * @param sysfilePath - Path for the index files - must exist
   * @param writeable
   * @throws IndexException
   */
  public BwIndexSolrImpl(final String sysfilePath,
                         final boolean writeable) throws IndexException {
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#setBatchSize(int)
   */
  public void setBatchSize(final int val) {
    batchMaxSize = val;
    batchCurSize = 0;
    if (batchMaxSize > 1) {
      batch = new XmlEmit();
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#endBwBatch()
   */
  public void endBwBatch() throws CalFacadeException {
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#flush()
   */
  public void flush() throws CalFacadeException {
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#getKeys(int, edu.rpi.cct.misc.indexing.Index.Key[])
   */
  public int getKeys(final int n, final Index.Key[] keys) throws CalFacadeException {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#indexEntity(java.lang.Object)
   */
  public void indexEntity(final Object rec) throws CalFacadeException {
    /*
    try {
      StringWriter xmlWtr = null;
      XmlEmit xml;

      if (batchMaxSize > 0) {
        synchronized(batch) {
          xml = batch;
          index(xml, rec);

        if (batchMaxSize == 0) {
          xml.closeTag(solrTagAdd);
        }
      } else {
        xml = new XmlEmit();
        xmlWtr = new StringWriter();
        xml.startEmit(xmlWtr);
        index(xml, rec);
      }

      if (xmlWtr != null) {
        indexAndCommit(xmlWtr.toString());
      }
    } catch (IOException e) {
      throw new CalFacadeException(e);
    }
    */
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#unindexEntity(java.lang.Object)
   */
  public void unindexEntity(final Object rec) throws CalFacadeException {
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#search(java.lang.String, edu.rpi.cct.misc.indexing.SearchLimits)
   */
  public int search(final String query, final SearchLimits limits) throws CalFacadeException {
    return 0;
  }

  /** Called to make or fill in a Key object.
   *
   * @param key   Possible Index.Key object for reuse
   * @param doc   The retrieved Document
   * @param score The rating for this entry
   * @return Index.Key  new or reused object
   * @throws IndexException
   */
  public Index.Key makeKey(Index.Key key,
                           final Document doc,
                           final float score) throws IndexException {
    if ((key == null) || (!(key instanceof BwIndexKey))) {
      key = new BwIndexKey();
    }

    BwIndexKey bwkey = (BwIndexKey)key;

    bwkey.setScore(score);

    String itemType = doc.get(BwIndexLuceneDefs.itemTypeInfo.getName());
    bwkey.setItemType(itemType);

    if (itemType == null) {
      throw new IndexException("org.bedework.index.noitemtype");
    }

    if (itemType.equals(BwIndexLuceneDefs.itemTypeCalendar)) {
      bwkey.setCalendarKey(doc.get(BwIndexLuceneDefs.keyCalendar.getName()));
    } else if (itemType.equals(BwIndexLuceneDefs.itemTypeEvent)) {
      bwkey.setEventKey(doc.get(BwIndexLuceneDefs.keyEvent.getName()));
    } else {
      throw new IndexException(IndexException.unknownRecordType,
                               itemType);
    }

    return key;
  }

  /** Called to make a key term for a record.
   *
   * @param   rec      The record
   * @return  Term     Lucene term which uniquely identifies the record
   * @throws IndexException
   */
  public Term makeKeyTerm(final Object rec) throws IndexException {
    String name = makeKeyName(rec);
    String key = makeKeyVal(rec);

    return new Term(name, key);
  }

  /** Called to make a key value for a record.
   *
   * @param   rec      The record
   * @return  String   String which uniquely identifies the record
   * @throws IndexException
   */
  public String makeKeyVal(final Object rec) throws IndexException {
    if (rec instanceof BwCalendar) {
      return ((BwCalendar)rec).getPath();
    }

    if (rec instanceof BwEvent) {
      BwEvent ev = (BwEvent)rec;

      String path = ev.getColPath();
      String guid = ev.getUid();
      String recurid = ev.getRecurrenceId();

      return keyConverter.makeEventKey(path, guid, recurid);
    }

    throw new IndexException(IndexException.unknownRecordType,
                             rec.getClass().getName());
  }

  /** Called to make the primary key name for a record.
   *
   * @param   rec      The record
   * @return  String   Name for the field/term
   * @throws IndexException
   */
  public String makeKeyName(final Object rec) throws IndexException {
    if (rec instanceof BwCalendar) {
      return BwIndexLuceneDefs.keyCalendar.getName();
    }

    if (rec instanceof BwEvent) {
      return BwIndexLuceneDefs.keyEvent.getName();
    }

    throw new IndexException(IndexException.unknownRecordType,
                             rec.getClass().getName());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.indexing.BwIndexer#setCleanLocks(boolean)
   */
  public void setCleanLocks(final boolean val) {
  }

  private synchronized void batchedIndex(final Object rec) throws CalFacadeException {
    try {
      if (batchCurSize == 0) {
        batch.startEmit(new StringWriter());
      }
      index(batch, rec);
    } catch (IOException e) {
      throw new CalFacadeException(e);
    }
  }

  private void index(final XmlEmit xml, final Object rec) throws CalFacadeException {
    try {
      if (batchCurSize == 0) {
        xml.openTag(solrTagAdd);
      }

      if (batchMaxSize == 0) {
        xml.closeTag(solrTagAdd);
      }
    } catch (IOException e) {
      throw new CalFacadeException(e);
    }
  }

  private void indexAndCommit(final String indexInfo) throws CalFacadeException {
  }
}
