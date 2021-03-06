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

import org.bedework.caldav.util.filter.FilterBase;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwDateTime;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.BwEventProxy;
import org.bedework.calfacade.BwRecurrenceInstance;
import org.bedework.calfacade.RecurringRetrievalMode;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.util.ChangeTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/** This is the events section of the low level interface to the calendar
 * database.
 *
 * @author Mike Douglass   douglm@rpi.edu
 */
public interface CoreEventsI extends Serializable {

  /* ====================================================================
   *                   Events
   * ==================================================================== */

  /** Return calendars which contain
   * @param guid
   * @param rid
   * @return Collection of calendar objects.
   * @throws CalFacadeException
   */
  public Collection<BwCalendar> findCalendars(String guid,
                                              String rid) throws CalFacadeException;

  /** Return one or more events using the calendar, guid and optionally a
   * recurrence-id as a key.
   *
   * <p>For most calendar collections one and only one event should be returned
   * for any given guid. However, for certain special collections (inbox etc)
   * the guid rules are relaxed.
   *
   * <p>For recurring events, the guid defines the 'master' event defining
   * the rules together with any exceptions.
   *
   * <p>The recurrence id defines a particular instance of a recurrence.
   *
   * <p>To specify the master entry provide a null recurrenceId or use the
   * recurRetrieval parameter. One CoreEventInfo object per recurring event will
   * be returned as for getEvent using the name.
   *
   * <p>if the rerieval mode is to expand recurrences, the Collection returned
   * will include all appropriate instances.
   *
   * @param colPath   String collection path or null.
   * @param guid      String guid for the event
   * @param rid       String recurrence id, null for non-recurring, null valued for
   *                    master or non-null-valued for particular occurrence.
   * @param scheduling true if this is us manipulating the inbox.
   * @param recurRetrieval How recurring event is returned.
   * @return  Collection of CoreEventInfo objects representing event(s).
   * @throws CalFacadeException
   */
  public Collection<CoreEventInfo> getEvent(String colPath,
                                            String guid, String rid,
                                            boolean scheduling,
                                            RecurringRetrievalMode recurRetrieval)
          throws CalFacadeException;

  /** Return the events for the current user within the given date/time
   * range.
   *
   * @param calendars    BwCalendar objects restricting search or null.
   *                     Each object must be a calendar collection. All aliases
   *                     must have been resolved.
   * @param filter       BwFilter object restricting search or null.
   * @param startDate    BwDateTime start - may be null
   * @param endDate      BwDateTime end - may be null.
   * @param retrieveList List of properties to return. Null means return all.
   * @param recurRetrieval How recurring event is returned.
   * @param freeBusy     Return skeleton events with date/times and skip
   *                     transparent events.
   * @return Collection  of CoreEventInfo objects
   * @throws CalFacadeException
   */
  public Collection<CoreEventInfo> getEvents(Collection <BwCalendar> calendars,
                                             FilterBase filter,
                                             BwDateTime startDate, BwDateTime endDate,
                                             List<String> retrieveList,
                                             RecurringRetrievalMode recurRetrieval,
                                             boolean freeBusy) throws CalFacadeException;

  /** Get an event given the calendar and String name. Return null for not
   * found. For non-recurring there should be only one event. For recurring
   * events, overrides and possibly instances will be returned.
   *
   * @param colPath    String collection path or null.
   * @param val        String possible name
   * @param recurRetrieval How recurring event is returned.
   * @return CoreEventInfo or null
   * @throws CalFacadeException
   */
  public CoreEventInfo getEvent(String colPath,
                                String val,
                                RecurringRetrievalMode recurRetrieval)
          throws CalFacadeException;

  /** Result from add or update event
   * We need to know what instances and overrrides were added or removed for
   * scheduling at least.
   *
   * <p>Any or all of these fields may be null
   *
   * @author Mike Douglass
   */
  public static class UpdateEventResult {
    /** True if the event was added or updated.
     */
    public boolean addedUpdated;

    /** Non-null for an error
     */
    public String errorCode;

    /** null for no failures or overrides which did not match.
     */
    public Collection<BwEventProxy> failedOverrides;

    /** These have been changed in some way */
    public List<BwRecurrenceInstance> updated;

    /** These have been deleted */
    public List<BwRecurrenceInstance> deleted;

    /** These have been added */
    public List<BwRecurrenceInstance> added;

    /**
     * @param val
     */
    public void addUpdated(final BwRecurrenceInstance val) {
      if (updated == null) {
        updated = new ArrayList<BwRecurrenceInstance>();
      }
      updated.add(val);
    }

    /**
     * @param val
     */
    public void addDeleted(final BwRecurrenceInstance val) {
      if (deleted == null) {
        deleted = new ArrayList<BwRecurrenceInstance>();
      }
      deleted.add(val);
    }

    /**
     * @param val
     */
    public void addAdded(final BwRecurrenceInstance val) {
      if (added == null) {
        added = new ArrayList<BwRecurrenceInstance>();
      }
      added.add(val);
    }
  }

  /** Add an event to the database. The id and uid will be set in the parameter
   * object.
   *
   * @param val   BwEvent object to be added
   * @param overrides    Collection of BwEventProxy objects which override instances
   *                     of the new event
   * @param scheduling   True if we are adding an event to an inbox for scheduling.
   * @param rollbackOnError true if we rollback and throw an exception on error
   * @return UpdateEventResult
   * @throws CalFacadeException
   */
  public UpdateEventResult addEvent(BwEvent val,
                                    Collection<BwEventProxy> overrides,
                                    boolean scheduling,
                                    boolean rollbackOnError) throws CalFacadeException;

  /** Update an event in the database.
   *
   * <p>This method will set any synchronization state entries to modified
   * unless we are synchronizing in which case that belonging to the current
   * user is set to mark the event as synchronized
   *
   * @param val   BwEvent object to be replaced
   * @param overrides
   * @param deletedOverrides - overrides that need to be deleted
   * @param changes
   * @return indication of changes made to overrides.
   * @exception CalFacadeException If there's a db problem or problem with
   *     the event
   */
  public UpdateEventResult updateEvent(BwEvent val,
                                       Collection<BwEventProxy> overrides,
                                       Collection<BwEventProxy> deletedOverrides,
                                       ChangeTable changes) throws CalFacadeException;

  /** This class allows the implementations to pass back some information
   * about what happened. If possible it should fill in the supplied fields.
   *
   * A result of zero for counts does not necessarily indicate nothing
   * happened, for example, the implementation may store elarms as part of
   * the event object and they just go as part of event deletion.
   */
  public static class DelEventResult {
    /**  false if it didn't exist
     */
    public boolean eventDeleted;

    /** Number of alarms deleted
     */
    public int alarmsDeleted;

    /** Constructor
     *
     * @param eventDeleted
     * @param alarmsDeleted
     */
    public DelEventResult(final boolean eventDeleted,
                          final int alarmsDeleted) {
      this.eventDeleted = eventDeleted;
      this.alarmsDeleted = alarmsDeleted;
    }
  }

  /** Delete an event and any associated alarms
   * Set any referring synch states to deleted.
   *
   * @param val                BwEvent object to be deleted
   * @param scheduling   True if we are deleting an event from an inbox for scheduling.
   * @param reallyDelete Really delete it - otherwise it's tombstoned
   * @return DelEventResult    result.
   * @exception CalFacadeException If there's a database access problem
   */
  public DelEventResult deleteEvent(BwEvent val,
                                    boolean scheduling,
                                    boolean reallyDelete) throws CalFacadeException;

  /** Move an event. Allows us to keep track for synch-report
   *
   * @param val            BwEvent object to be moved
   * @param from           Path of currrent collection
   * @param to             Wheer it's going
   * @exception CalFacadeException If there's a database access problem
   */
  public void moveEvent(BwEvent val,
                        String from,
                        String to) throws CalFacadeException;

  /** XXX temp I think
   * Retrieve event proxies in the given calendar - they will be used to remove events
   * from result sets.
   *
   * @param cal     BwCalendar object restricting search or null.
   * @return Collection of CoreEventInfo objects
   * @throws CalFacadeException
   */
  public Collection<CoreEventInfo> getDeletedProxies(BwCalendar cal)
          throws CalFacadeException;

  /** This represents an internal key to an event.
   *
   */
  public abstract static class InternalEventKey implements Serializable {
    /**
     * @return start time for indexed event
     */
    public abstract BwDateTime getStart();

    /**
     * @return end time for indexed event
     */
    public abstract BwDateTime getEnd();

    /**
     * @return owner href for indexed event
     */
    public abstract String getOwnerHref();
  }

  /** Return all keys or all with a lastmod greater than or equal to that supplied.
   *
   * <p>The lastmod allows us to redo the search after we have updated timezones
   * to find all events added after we made the last call.
   *
   * <p>Note the lastmod has a coarse granularity so it may need to be backed off
   * to ensure all events are covered if doing batches.
   *
   * @param lastmod
   * @return collection of opaque key objects.
   * @throws CalFacadeException
   */
  public Collection<? extends InternalEventKey> getEventKeysForTzupdate(String lastmod)
          throws CalFacadeException;

  /** Get an event given the internal key. Returns null if event no longer
   * exists.
   *
   * @param key
   * @return CoreEventInfo
   * @throws CalFacadeException
   */
  public CoreEventInfo getEvent(InternalEventKey key)
          throws CalFacadeException;

  /** Return all events on the given path with a lastmod GREATER
   * THAN that supplied. The path may not be null. A null lastmod will
   * return all events in the collection.
   *
   * @param path - must be non-null
   * @param lastmod - limit search, may be null
   * @return list of events.
   * @throws CalFacadeException
   */
  public Set<CoreEventInfo> getSynchEvents(String path,
                                           String lastmod) throws CalFacadeException;

  /* ====================================================================
   *                  Admin support
   * ==================================================================== */

  /** Obtain the next batch of children names for the supplied path. A path of
   * null will return the system roots. Tese are the names of stored entities,
   * NOT the paths.
   *
   * @param parentPath
   * @param start start index in the batch - 0 for the first
   * @param count count of results we want
   * @return collection of String names or null for no more
   * @throws CalFacadeException
   */
  public Collection<String> getChildEntities(String parentPath,
                                             int start,
                                             int count) throws CalFacadeException;
}
