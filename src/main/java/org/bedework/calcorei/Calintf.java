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
package org.bedework.calcorei;

import org.bedework.calfacade.BwAlarm;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwDateTime;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwStats;
import org.bedework.calfacade.BwStats.StatsEntry;
import org.bedework.calfacade.BwSystem;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.base.BwDbentity;
import org.bedework.calfacade.base.BwShareableDbentity;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.util.AccessUtilI;
import org.bedework.sysevents.events.SysEventBase;

import edu.rpi.cmt.access.Ace;
import edu.rpi.cmt.access.AceWho;
import edu.rpi.cmt.access.Acl.CurrentAccess;
import edu.rpi.cmt.access.PrivilegeSet;

import java.util.Collection;
import java.util.Properties;

/** This is the low level interface to the calendar database.
 *
 * <p>This interface provides a view of the data as seen by the supplied user
 * id. This may or may not be the actual authenticated user of whatever
 * application is driving it.
 *
 * <p>This is of particular use for public events administration. A given
 * authenticated user may be the member of a number of groups, and this module
 * will be initialised with the id of one of those groups. At some point
 * the authenticated user may choose to switch identities to manage a different
 * group.
 *
 * <p>The UserAuth object returned by getUserAuth usually represents the
 * authenticated user and determines the rights that user has.
 *
 * @author Mike Douglass   douglm@rpi.edu
 */
public interface Calintf extends CoreCalendarsI, CoreEventsI {
  /** Must be called before any db interactions.
   *
   * @param props       Properties used to control the underlying implementation
   * @throws CalFacadeException
   */
  public void initDb(Properties props) throws CalFacadeException;

  /** Must be called to initialize the new object.
   *
   * @param syspars
   * @param accessCb    Required for access evaluation.
   * @param url         String url to which we are connecting
   * @param user        BwUser user of the application
   * @param publicAdmin boolean true if this is a public events admin app
   * @param sessionless true if this is a sessionless client
   * @throws CalFacadeException
   */
  public void init(BwSystem syspars,
                   AccessUtilI.CallBack accessCb,
                   String url,
                   BwUser user,
                   boolean publicAdmin,
                   boolean sessionless) throws CalFacadeException;

  /** Called after init to flag this user as a super user.
   *
   * @param val       true for a super user
   */
  public void setSuperUser(boolean val);

  /**
   *
   * @return boolean true if super user
   */
  public boolean getSuperUser();

  /**
   * @param val
   */
  public void setMaximumAllowedPrivs(PrivilegeSet val);

  /** Get the current system (not db) stats
   *
   * @return BwStats object
   * @throws CalFacadeException if not admin
   */
  public BwStats getStats() throws CalFacadeException;

  /** Enable/disable db statistics
   *
   * @param enable       boolean true to turn on db statistics collection
   * @throws CalFacadeException if not admin
   */
  public void setDbStatsEnabled(boolean enable) throws CalFacadeException;

  /**
   *
   * @return boolean true if statistics collection enabled
   * @throws CalFacadeException if not admin
   */
  public boolean getDbStatsEnabled() throws CalFacadeException;

  /** Dump db statistics
   *
   * @throws CalFacadeException if not admin
   */
  public void dumpDbStats() throws CalFacadeException;

  /** Get db statistics
   *
   * @return Collection of BwStats.StatsEntry objects
   * @throws CalFacadeException if not admin
   */
  public Collection<StatsEntry> getDbStats() throws CalFacadeException;

  /** Get information about this interface
   *
   * @return CalintfInfo
   * @throws CalFacadeException
   */
  public CalintfInfo getInfo() throws CalFacadeException;

  /** Signal the start of a sequence of operations. These overlap transactions
   * in that there may be 0 to many transactions started and ended within an
   * open/close call and many open/close calls within a transaction.
   *
   * @param webMode  true for long-running multi request conversations.
   * @throws CalFacadeException
   */
  public void open(boolean webMode) throws CalFacadeException;

  /** Call on the way out after handling a request..
   *
   * @throws CalFacadeException
   */
  public void close() throws CalFacadeException;

  /** Start a (possibly long-running) transaction. In the web environment
   * this might do nothing. The endTransaction method should in some way
   * check version numbers to detect concurrent updates and fail with an
   * exception.
   *
   * @throws CalFacadeException
   */
  public void beginTransaction() throws CalFacadeException;

  /** End a (possibly long-running) transaction. In the web environment
   * this should in some way check version numbers to detect concurrent updates
   * and fail with an exception.
   *
   * @throws CalFacadeException
   */
  public void endTransaction() throws CalFacadeException;

  /** Call if there has been an error during an update process.
   *
   * @throws CalFacadeException
   */
  public void rollbackTransaction() throws CalFacadeException;

  /**
   * @return boolean true if open and rolled back
   * @throws CalFacadeException
   */
  public boolean isRolledback() throws CalFacadeException;

  /** Flush queued operations.
   *
   * @throws CalFacadeException
   */
  public void flush() throws CalFacadeException;

  /** An implementation specific method allowing access to the underlying
   * persisitance engine. This may return, for example, a Hibernate session,
   *
   * @return Object
   * @throws CalFacadeException
   */
  public Object getDbSession() throws CalFacadeException;

  /** Call to reassociate an entity with the current database session
   *
   * @param val
   * @throws CalFacadeException
   */
  public void reAttach(BwDbentity val) throws CalFacadeException;

  /** Set the current system pars
   *
   * @param val BwSystem object
   * @throws CalFacadeException if not admin
   */
  public void setSyspars(BwSystem val) throws CalFacadeException;

  /** Get the current system pars
   *
   * @return BwSystem object
   * @throws CalFacadeException if not admin
   */
  public BwSystem getSyspars() throws CalFacadeException;

  /* ====================================================================
   *                   Notifications
   * ==================================================================== */

  /** Called to notify container that an event occurred. This method should
   * queue up notifications until after transaction commit as consumers
   * should only receive notifications when the actual data has been written.
   *
   * @param ev
   * @throws CalFacadeException
   */
  public void postNotification(final SysEventBase ev) throws CalFacadeException;

  /** Called to flush any queued notifications. Called by the commit
   * process.
   *
   * @throws CalFacadeException
   */
  public void flushNotifications() throws CalFacadeException;

  /** Clear any queued notifications without posting. Called by the commit
   * process.
   *
   * @throws CalFacadeException
   */
  public void clearNotifications() throws CalFacadeException;

  /* ====================================================================
   *                   Access
   * ==================================================================== */

  /** Change the access to the given calendar entity.
   *
   * @param ent      BwShareableDbentity
   * @param aces     Collection of ace
   * @param replaceAll true to replace the entire access list.
   * @throws CalFacadeException
   */
  public void changeAccess(BwShareableDbentity ent,
                           Collection<Ace> aces,
                           boolean replaceAll) throws CalFacadeException;

  /** Remove any explicit access for the given who to the given calendar entity.
  *
  * @param ent      BwShareableDbentity
  * @param who      AceWho
  * @throws CalFacadeException
  */
 public abstract void defaultAccess(BwShareableDbentity ent,
                                    AceWho who) throws CalFacadeException;

 /** Return a Collection of the objects after checking access
  *
  * @param ents          Collection of BwShareableDbentity
  * @param desiredAccess access we want
  * @param alwaysReturn boolean flag behaviour on no access
  * @return Collection   of checked objects
  * @throws CalFacadeException for no access or other failure
  */
 public Collection<? extends BwShareableDbentity<? extends Object>>
                checkAccess(Collection<? extends BwShareableDbentity<? extends Object>> ents,
                               int desiredAccess,
                               boolean alwaysReturn)
         throws CalFacadeException;

  /** Check the access for the given entity. Returns the current access
   * or null or optionally throws a no access exception.
   *
   * @param ent
   * @param desiredAccess
   * @param returnResult
   * @return CurrentAccess
   * @throws CalFacadeException if returnResult false and no access
   */
  public CurrentAccess checkAccess(BwShareableDbentity ent, int desiredAccess,
                                   boolean returnResult) throws CalFacadeException;

  /* ====================================================================
   *                   Alarms
   * ==================================================================== */

  /** Return all unexpired alarms before a given time. If time is 0 all
   * unexpired alarms will be retrieved.
   *
   * <p>Any cancelled alarms will be excluded from the result.
   *
   * <p>Typically the system will call this with a time set into the near future
   * and then queue up alarms that are near to triggering.
   *
   * @param triggerTime
   * @return Collection of unexpired alarms.
   * @throws CalFacadeException
   */
  public abstract Collection<BwAlarm> getUnexpiredAlarms(long triggerTime)
          throws CalFacadeException;

  /** Given an alarm return the associated event(s)
   *
   * @param alarm
   * @return an event.
   * @throws CalFacadeException
   */
  public abstract Collection<BwEvent> getEventsByAlarm(BwAlarm alarm)
          throws CalFacadeException;

  /* ====================================================================
   *                   Free busy
   * ==================================================================== */

  /** Get the fee busy for calendars (if cal != null) or for a principal.
   *
   * @param cals
   * @param who
   * @param start
   * @param end
   * @param returnAll
   * @param ignoreTransparency
   * @return  BwFreeBusy object representing the calendar (or principal's)
   *          free/busy
   * @throws CalFacadeException
   */
  public BwEvent getFreeBusy(Collection<BwCalendar> cals, BwPrincipal who,
                             BwDateTime start, BwDateTime end,
                             boolean returnAll,
                             boolean ignoreTransparency)
          throws CalFacadeException;
}

