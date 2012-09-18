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

import org.bedework.calfacade.exc.CalFacadeException;

import edu.rpi.cct.misc.indexing.Index;
import edu.rpi.cct.misc.indexing.SearchLimits;

import java.io.Serializable;

/**
 * @author douglm
 *
 */
public interface BwIndexer extends Serializable {

  /** Called to find entries that match the search string. This string may
   * be a simple sequence of keywords or some sort of query the syntax of
   * which is determined by the underlying implementation.
   *
   * @param   query        Query string
   * @param   limits       Search limits to apply or null
   * @return  int          Number found. 0 means none found,
   *                                -1 means indeterminate
   * @throws CalFacadeException
   */
  public int search(String query, SearchLimits limits) throws CalFacadeException;

  /** Called to unindex a record
   *
   * @param   rec      The record to unindex
   * @throws CalFacadeException
   */
  public void unindexEntity(Object rec) throws CalFacadeException;

  /** Called to index a record
   *
   * @param rec
   * @throws CalFacadeException
   */
  public void indexEntity(Object rec) throws CalFacadeException;

  /** Called to retrieve record keys from the result.
   *
   * @param   n        Starting index
   * @param   keys     Array for the record keys
   * @return  int      Actual number of records
   * @throws CalFacadeException
   */
  public int getKeys(int n, Index.Key[] keys) throws CalFacadeException;


  /** Set to > 1 to enable batching
   *
   * @param val
   */
  public void setBatchSize(int val);

  /** Called at the end of a batch of updates.
   *
   * @throws CalFacadeException
   */
  public void endBwBatch() throws CalFacadeException;

  /** Flush any batched entities.
   * @throws CalFacadeException
   */
  public void flush() throws CalFacadeException;

  /** Indicate if we should try to clean locks.
   *
   * @param val
   */
  public void setCleanLocks(boolean val);
}
