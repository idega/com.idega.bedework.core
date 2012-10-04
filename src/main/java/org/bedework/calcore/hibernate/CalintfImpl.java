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
package org.bedework.calcore.hibernate;

import org.bedework.calcore.AccessUtil;
import org.bedework.calcore.CalintfBase;
import org.bedework.calcorei.CalintfInfo;
import org.bedework.calcorei.CoreEventInfo;
import org.bedework.calcorei.CoreEventsI;
import org.bedework.calcorei.HibSession;
import org.bedework.caldav.util.filter.EntityTypeFilter;
import org.bedework.caldav.util.filter.FilterBase;
import org.bedework.caldav.util.filter.OrFilter;
import org.bedework.calfacade.BwAlarm;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwDateTime;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.BwEventObj;
import org.bedework.calfacade.BwEventProxy;
import org.bedework.calfacade.BwFreeBusyComponent;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwStats;
import org.bedework.calfacade.BwStats.StatsEntry;
import org.bedework.calfacade.BwSystem;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.CollectionSynchInfo;
import org.bedework.calfacade.RecurringRetrievalMode;
import org.bedework.calfacade.RecurringRetrievalMode.Rmode;
import org.bedework.calfacade.base.BwDbentity;
import org.bedework.calfacade.base.BwShareableDbentity;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.util.AccessUtilI;
import org.bedework.calfacade.util.ChangeTable;
import org.bedework.calfacade.util.Granulator.EventPeriod;
import org.bedework.calfacade.wrappers.CalendarWrapper;
import org.bedework.sysevents.events.SysEvent;

import edu.rpi.cmt.access.Ace;
import edu.rpi.cmt.access.AceWho;
import edu.rpi.cmt.access.Acl.CurrentAccess;
import edu.rpi.cmt.access.PrivilegeDefs;
import edu.rpi.cmt.access.PrivilegeSet;
import edu.rpi.cmt.calendar.IcalDefs;

import net.fortuna.ical4j.model.DateTime;
import net.fortuna.ical4j.model.Period;

import org.hibernate.FlushMode;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;

import com.idega.hibernate.SessionFactoryUtil;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Implementation of CalIntf which uses hibernate as its persistance engine.
 * 
 * <p>
 * We assume this interface is accessing public events or private events. In
 * either case it may be read-only or read/write. It is up to the caller to set
 * the appropriate access.
 * 
 * <p>
 * Write access to public objects may be restricted to only those owned by the
 * supplied owner. It is up to the caller to determine the setting for the
 * modifyAll flag.
 * 
 * <p>
 * The following methods always work within the above context, e.g. 'all' for an
 * object initialised for public access means all public objects of a requested
 * class. For a personal object it means all objects owned by the current user.
 * 
 * <p>
 * Currently some classes are only available as public objects. This might
 * change.
 * 
 * <p>
 * A public object in readonly mode will deliver all public objects within the
 * given constraints, regardless of ownership, e.g all events for given day or
 * all public categories.
 * 
 * <p>
 * A public object in read/write will enforce ownership on display and on
 * update. This might require a client to obtain two or more objects to get the
 * appropriate behaviour. For example, an admin client will only allow update of
 * events owned by the current user but must display all public categories for
 * use.
 * 
 * @author Mike Douglass douglm@rpi.edu
 */
public class CalintfImpl extends CalintfBase implements PrivilegeDefs {
	private static BwStats stats = new BwStats();

	private static CalintfInfo info = new CalintfInfo(true, // handlesLocations
			true, // handlesContacts
			true // handlesCategories
	);

	/**
	 * For evaluating access control
	 */
	private AccessUtil access;

	private CoreEventsI events;

	private CoreCalendars calendars;

	/**
	 * Prevent updates.
	 */
	// sprivate boolean readOnly;

	/**
	 * Current hibernate session - exists only across one user interaction
	 */
	private HibSession sess;

	/**
	 * We make this static for this implementation so that there is only one
	 * SessionFactory per server for the calendar.
	 * 
	 * <p>
	 * static fields used this way are illegal in the j2ee specification though
	 * we might get away with it here as the session factory only contains
	 * parsed mappings for the calendar configuration. This should be the same
	 * for any machine in a cluster so it might work OK.
	 * 
	 * <p>
	 * It might be better to find some other approach for the j2ee world.
	 */
	private static SessionFactory sessionFactory;
	private static Statistics dbStats;

	/*
	 * ====================================================================
	 * initialisation
	 * ====================================================================
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcore.CalintfBase#init(org.bedework.calfacade.BwSystem,
	 * org.bedework.calfacade.util.AccessUtilI.CallBack, java.lang.String,
	 * org.bedework.calfacade.BwUser, boolean, boolean)
	 */
	@Override
	public void init(final BwSystem syspars,
			final AccessUtilI.CallBack accessCb, final String url,
			final BwUser user, final boolean publicAdmin,
			final boolean sessionless) throws CalFacadeException {
		super.init(syspars, accessCb, url, user, publicAdmin, sessionless);

		try {
			access = new AccessUtil();
			access.init(accessCb);
		} catch (Throwable t) {
			throw new CalFacadeException(t);
		}

		access.setAuthUser(getUser());

		CalintfHelperCallback cb = new CalintfHelperCallback(this, getUser());
		CalintfHelperHib.CalintfHelperHibCb chcb = new CalintfHelperHibCb(this);

		events = new CoreEvents(chcb, cb, access, currentMode, sessionless);

		calendars = new CoreCalendars(chcb, cb, access, currentMode,
				sessionless);

		access.setCollectionGetter(calendars);
	}

	private static class CalintfHelperHibCb implements
			CalintfHelperHib.CalintfHelperHibCb {
		private CalintfImpl intf;

		CalintfHelperHibCb(final CalintfImpl intf) {
			this.intf = intf;
		}

		public HibSession getSess() throws CalFacadeException {
			return (HibSession) intf.getDbSession();
		}

		public BwStats getStats() throws CalFacadeException {
			return intf.getStats();
		}

		public BwCalendar getCollection(final String path)
				throws CalFacadeException {
			try {
				return intf.calendars.getCalendar(path, Ace.privAny, true);
			} catch (Throwable t) {
				if (t instanceof CalFacadeException) {
					throw (CalFacadeException) t;
				}

				throw new CalFacadeException(t);
			}
		}

		public BwCalendar getCollection(final String path,
				final int desiredAccess, final boolean alwaysReturn)
				throws CalFacadeException {
			try {
				return intf.calendars.getCalendar(path, desiredAccess,
						alwaysReturn);
			} catch (Throwable t) {
				if (t instanceof CalFacadeException) {
					throw (CalFacadeException) t;
				}

				throw new CalFacadeException(t);
			}
		}

		public void postNotification(final SysEvent ev)
				throws CalFacadeException {
			intf.postNotification(ev);
		}
	}

	private static class CalintfHelperCallback implements
			CalintfHelperHib.Callback {
		private CalintfBase intf;
		private BwUser user;

		CalintfHelperCallback(final CalintfBase intf, final BwUser user) {
			this.intf = intf;
			this.user = user;
		}

		public void rollback() throws CalFacadeException {
			intf.rollbackTransaction();
		}

		public BwSystem getSyspars() throws CalFacadeException {
			return intf.getSyspars();
		}

		public BwUser getUser() throws CalFacadeException {
			return user;
		}

		public boolean getSuperUser() throws CalFacadeException {
			return intf.getSuperUser();
		}

		public Collection<BwCalendar> getCalendars(final BwCalendar cal)
				throws CalFacadeException {
			return intf.getCalendars(cal);
		}
	}

	public void setSuperUser(final boolean val) {
		access.setSuperUser(val);
	}

	public boolean getSuperUser() {
		return access.getSuperUser();
	}

	public void setMaximumAllowedPrivs(final PrivilegeSet val) {
		access.setMaximumAllowedPrivs(val);
	}

	public BwStats getStats() throws CalFacadeException {
		if (stats == null) {
			return null;
		}

		return stats;
	}

	public void setDbStatsEnabled(final boolean enable)
			throws CalFacadeException {
		if (!enable && (dbStats == null)) {
			return;
		}

		if (dbStats == null) {
			dbStats = getSessionFactory().getStatistics();
		}

		dbStats.setStatisticsEnabled(enable);
	}

	public boolean getDbStatsEnabled() throws CalFacadeException {
		if (dbStats == null) {
			return false;
		}

		return dbStats.isStatisticsEnabled();
	}

	public void dumpDbStats() throws CalFacadeException {
		DbStatistics.dumpStats(dbStats);
	}

	public Collection<StatsEntry> getDbStats() throws CalFacadeException {
		return DbStatistics.getStats(dbStats);
	}

	public CalintfInfo getInfo() throws CalFacadeException {
		return info;
	}

	/*
	 * ==================================================================== Misc
	 * methods
	 * ====================================================================
	 */

	public synchronized void open(final boolean webMode)
			throws CalFacadeException {
		if (isOpen) {
			throw new CalFacadeException("Already open");
		}

		isOpen = true;

		if ((sess != null) && (!webMode)) {
			warn("Session is not null. Will close");
			try {
				close();
			} finally {
			}
		}

		if (sess == null) {
			if (debug) {
				debug("New hibernate session for " + objTimestamp);
			}
			sess = new HibSessionImpl();
			sess.init(getSessionFactory(), getLogger());
			if (webMode) {
				sess.setFlushMode(FlushMode.MANUAL);
			} else if (debug) {
				debug("Open session for " + objTimestamp);
			}
		}

		if (access != null) {
			access.open();
		}
	}

	public synchronized void close() throws CalFacadeException {
		if (!isOpen) {
			if (debug) {
				debug("Close for " + objTimestamp + " closed session");
			}
			return;
		}

		if (debug) {
			debug("Close for " + objTimestamp);
		}

		try {
			if (sess != null) {
				if (sess.rolledback()) {
					sess.close();
					sess = null;
					return;
				}

				if (sess.transactionStarted()) {
					sess.rollback();
				}
				// sess.disconnect();
				sess.close();
				sess = null;
			}
		} catch (Throwable t) {
			try {
				sess.close();
			} catch (Throwable t1) {
			}
			sess = null; // Discard on error
		} finally {
			isOpen = false;
		}

		if (access != null) {
			access.close();
			flushNotifications();
		}
	}

	public void beginTransaction() throws CalFacadeException {
		checkOpen();
		// sess.close();
		if (debug) {
			debug("Begin transaction for " + objTimestamp);
		}
		sess.beginTransaction();

		if (events != null) {
			((CalintfHelperHib) events).startTransaction();
		}

		if (calendars != null) {
			((CalintfHelperHib) calendars).startTransaction();
		}
	}

	public void endTransaction() throws CalFacadeException {
		try {
			checkOpen();

			if (debug) {
				debug("End transaction for " + objTimestamp);
			}

			if (!sess.rolledback()) {
				sess.commit();
			}

			if (events != null) {
				((CalintfHelperHib) events).endTransaction();
			}

			if (calendars != null) {
				((CalintfHelperHib) calendars).endTransaction();
			}
		} catch (CalFacadeException cfe) {
			sess.rollback();
			throw cfe;
		} catch (Throwable t) {
			sess.rollback();
			throw new CalFacadeException(t);
		} finally {
			flushNotifications();
		}
	}

	public void rollbackTransaction() throws CalFacadeException {
		try {
			checkOpen();
			sess.rollback();
		} finally {
			clearNotifications();
		}
	}

	public boolean isRolledback() throws CalFacadeException {
		if (!isOpen) {
			return false;
		}

		return sess.rolledback();
	}

	public void flush() throws CalFacadeException {
		if (debug) {
			getLogger().debug("flush for " + objTimestamp);
		}
		if (sess.isOpen()) {
			sess.flush();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.Calintf#getDbSession()
	 */
	public Object getDbSession() throws CalFacadeException {
		return sess;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.Calintf#reAttach(org.bedework.calfacade.base.BwDbentity
	 * )
	 */
	public void reAttach(BwDbentity val) throws CalFacadeException {
		if (val instanceof CalendarWrapper) {
			CalendarWrapper ccw = (CalendarWrapper) val;
			val = ccw.fetchEntity();
		}
		sess.reAttach(val);
	}

	/*
	 * ====================================================================
	 * Access
	 * ====================================================================
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.Calintf#changeAccess(org.bedework.calfacade.base
	 * .BwShareableDbentity, java.util.Collection, boolean)
	 */
	public void changeAccess(final BwShareableDbentity ent,
			final Collection<Ace> aces, final boolean replaceAll)
			throws CalFacadeException {
		if (ent instanceof BwCalendar) {
			changeAccess((BwCalendar) ent, aces, replaceAll);
			return;
		}
		checkOpen();
		access.changeAccess(ent, aces, replaceAll);
		sess.saveOrUpdate(ent);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CalendarsI#changeAccess(org.bedework.calfacade.
	 * BwCalendar, java.util.Collection)
	 */
	public void changeAccess(final BwCalendar cal, final Collection<Ace> aces,
			final boolean replaceAll) throws CalFacadeException {
		checkOpen();
		calendars.changeAccess(cal, aces, replaceAll);
	}

	public void defaultAccess(final BwShareableDbentity ent, final AceWho who)
			throws CalFacadeException {
		if (ent instanceof BwCalendar) {
			defaultAccess((BwCalendar) ent, who);
			return;
		}
		checkOpen();
		checkAccess(ent, privWriteAcl, false);
		access.defaultAccess(ent, who);
		sess.saveOrUpdate(ent);
	}

	public void defaultAccess(final BwCalendar cal, final AceWho who)
			throws CalFacadeException {
		checkOpen();
		calendars.defaultAccess(cal, who);
	}

	public Collection<? extends BwShareableDbentity<? extends Object>> checkAccess(
			final Collection<? extends BwShareableDbentity<? extends Object>> ents,
			final int desiredAccess, final boolean alwaysReturn)
			throws CalFacadeException {
		return access.checkAccess(ents, desiredAccess, alwaysReturn);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.Calintf#checkAccess(org.bedework.calfacade.base
	 * .BwShareableDbentity, int, boolean)
	 */
	public CurrentAccess checkAccess(final BwShareableDbentity ent,
			final int desiredAccess, final boolean returnResult)
			throws CalFacadeException {
		return access.checkAccess(ent, desiredAccess, returnResult);
	}

	/*
	 * ====================================================================
	 * Alarms
	 * ====================================================================
	 */

	@SuppressWarnings("unchecked")
	public Collection<BwAlarm> getUnexpiredAlarms(final long triggerTime)
			throws CalFacadeException {
		checkOpen();

		if (triggerTime == 0) {
			sess.namedQuery("getUnexpiredAlarms");
		} else {
			sess.namedQuery("getUnexpiredAlarmsTime");
			sess.setString("tt", String.valueOf(triggerTime));
		}

		return sess.getList();
	}

	@SuppressWarnings("unchecked")
	public Collection<BwEvent> getEventsByAlarm(final BwAlarm alarm)
			throws CalFacadeException {
		checkOpen();

		sess.namedQuery("eventByAlarm");
		sess.setInt("alarmId", alarm.getId());

		return sess.getList();
	}

	/*
	 * ====================================================================
	 * Calendars
	 * ====================================================================
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.CoreCalendarsI#getSynchInfo(java.lang.String,
	 * java.lang.String)
	 */
	public CollectionSynchInfo getSynchInfo(final String path,
			final String token) throws CalFacadeException {
		checkOpen();

		return calendars.getSynchInfo(path, token);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CoreCalendarsI#getCalendars(org.bedework.calfacade
	 * .BwCalendar)
	 */
	public Collection<BwCalendar> getCalendars(final BwCalendar cal)
			throws CalFacadeException {
		checkOpen();

		return calendars.getCalendars(cal);
	}

	public BwCalendar resolveAlias(final BwCalendar val,
			final boolean resolveSubAlias, final boolean freeBusy)
			throws CalFacadeException {
		checkOpen();

		return calendars.resolveAlias(val, resolveSubAlias, freeBusy);
	}

	public BwCalendar getCalendar(final String path, final int desiredAccess,
			final boolean alwaysReturnResult) throws CalFacadeException {
		checkOpen();

		return calendars.getCalendar(path, desiredAccess, alwaysReturnResult);
	}

	public String getDefaultCalendarPath(final BwUser user)
			throws CalFacadeException {
		return calendars.getDefaultCalendarPath(user);
	}

	public String getPrincipalRootPath(final BwPrincipal owner)
			throws CalFacadeException {
		return calendars.getPrincipalRootPath(owner);
	}

	public GetSpecialCalendarResult getSpecialCalendar(final BwPrincipal owner,
			final int calType, final boolean create, final int access)
			throws CalFacadeException {
		return calendars.getSpecialCalendar(owner, calType, create, access);
	}

	public BwCalendar add(final BwCalendar val, final String parentPath)
			throws CalFacadeException {
		checkOpen();

		return calendars.add(val, parentPath);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.CoreCalendarsI#touchCalendar(java.lang.String)
	 */
	public void touchCalendar(final String path) throws CalFacadeException {
		checkOpen();
		calendars.touchCalendar(path);
	}

	public void updateCalendar(final BwCalendar val) throws CalFacadeException {
		checkOpen();
		calendars.updateCalendar(val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CalendarsI#renameCalendar(org.bedework.calfacade
	 * .BwCalendar, java.lang.String)
	 */
	public void renameCalendar(final BwCalendar val, final String newName)
			throws CalFacadeException {
		checkOpen();
		calendars.renameCalendar(val, newName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CalendarsI#moveCalendar(org.bedework.calfacade.
	 * BwCalendar, org.bedework.calfacade.BwCalendar)
	 */
	public void moveCalendar(final BwCalendar val, final BwCalendar newParent)
			throws CalFacadeException {
		checkOpen();
		calendars.moveCalendar(val, newParent);
	}

	public boolean deleteCalendar(final BwCalendar val,
			final boolean reallyDelete) throws CalFacadeException {
		checkOpen();

		return calendars.deleteCalendar(val, reallyDelete);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.CoreCalendarsI#isEmpty(org.bedework.calfacade.
	 * BwCalendar)
	 */
	public boolean isEmpty(final BwCalendar val) throws CalFacadeException {
		checkOpen();

		return calendars.isEmpty(val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CalendarsI#addNewCalendars(org.bedework.calfacade
	 * .BwUser)
	 */
	public void addNewCalendars(final BwPrincipal user)
			throws CalFacadeException {
		checkOpen();

		calendars.addNewCalendars(user);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.CoreCalendarsI#getChildren(java.lang.String,
	 * int, int)
	 */
	public Collection<String> getChildCollections(final String parentPath,
			final int start, final int count) throws CalFacadeException {
		checkOpen();

		return calendars.getChildCollections(parentPath, start, count);
	}

	public Set<BwCalendar> getSynchCols(final String path, final String lastmod)
			throws CalFacadeException {
		return calendars.getSynchCols(path, lastmod);
	}

	public String getSyncToken(final String path) throws CalFacadeException {
		return calendars.getSyncToken(path);
	}

	public Collection<String> getChildEntities(final String parentPath,
			final int start, final int count) throws CalFacadeException {
		checkOpen();

		return events.getChildEntities(parentPath, start, count);
	}

	/*
	 * ==================================================================== Free
	 * busy ====================================================================
	 */

	public BwEvent getFreeBusy(final Collection<BwCalendar> cals,
			final BwPrincipal who, final BwDateTime start,
			final BwDateTime end, final boolean returnAll,
			final boolean ignoreTransparency) throws CalFacadeException {
		if (!(who instanceof BwUser)) {
			throw new CalFacadeException(
					"Unsupported: non user principal for free-busy");
		}

		Collection<CoreEventInfo> events = getFreeBusyEntities(cals, start,
				end, ignoreTransparency);
		BwEvent fb = new BwEventObj();

		fb.setEntityType(IcalDefs.entityTypeFreeAndBusy);
		fb.setOwnerHref(who.getPrincipalRef());
		fb.setDtstart(start);
		fb.setDtend(end);
		// assignGuid(fb);

		try {
			TreeSet<EventPeriod> eventPeriods = new TreeSet<EventPeriod>();

			for (CoreEventInfo ei : events) {
				BwEvent ev = ei.getEvent();

				// Ignore if times were specified and this event is outside the
				// times

				BwDateTime estart = ev.getDtstart();
				BwDateTime eend = ev.getDtend();

				/* Don't report out of the requested period */

				String dstart;
				String dend;

				if (estart.before(start)) {
					dstart = start.getDtval();
				} else {
					dstart = estart.getDtval();
				}

				if (eend.after(end)) {
					dend = end.getDtval();
				} else {
					dend = eend.getDtval();
				}

				DateTime psdt = new DateTime(dstart);
				DateTime pedt = new DateTime(dend);

				psdt.setUtc(true);
				pedt.setUtc(true);

				int type = BwFreeBusyComponent.typeBusy;

				if (BwEvent.statusTentative.equals(ev.getStatus())) {
					type = BwFreeBusyComponent.typeBusyTentative;
				}

				eventPeriods.add(new EventPeriod(psdt, pedt, type));
			}

			/*
			 * iterate through the sorted periods combining them where they are
			 * adjacent or overlap
			 */

			Period p = null;

			/*
			 * For the moment just build a single BwFreeBusyComponent
			 */
			BwFreeBusyComponent fbc = null;
			int lastType = 0;

			for (EventPeriod ep : eventPeriods) {
				if (debug) {
					trace(ep.toString());
				}

				if (p == null) {
					p = new Period(ep.getStart(), ep.getEnd());
					lastType = ep.getType();
				} else if ((lastType != ep.getType())
						|| ep.getStart().after(p.getEnd())) {
					// Non adjacent periods
					if (fbc == null) {
						fbc = new BwFreeBusyComponent();
						fbc.setType(lastType);
						fb.addFreeBusyPeriod(fbc);
					}
					fbc.addPeriod(p.getStart(), p.getEnd());

					if (lastType != ep.getType()) {
						fbc = null;
					}

					p = new Period(ep.getStart(), ep.getEnd());
					lastType = ep.getType();
				} else if (ep.getEnd().after(p.getEnd())) {
					// Extend the current period
					p = new Period(p.getStart(), ep.getEnd());
				} // else it falls within the existing period
			}

			if (p != null) {
				if ((fbc == null) || (lastType != fbc.getType())) {
					fbc = new BwFreeBusyComponent();
					fbc.setType(lastType);
					fb.addFreeBusyPeriod(fbc);
				}
				fbc.addPeriod(p.getStart(), p.getEnd());
			}
		} catch (Throwable t) {
			if (debug) {
				error(t);
			}
			throw new CalFacadeException(t);
		}

		return fb;
	}

	/*
	 * ====================================================================
	 * Events
	 * ====================================================================
	 */

	public Collection<CoreEventInfo> getEvents(
			final Collection<BwCalendar> calendars, final FilterBase filter,
			final BwDateTime startDate, final BwDateTime endDate,
			final List<String> retrieveList,
			final RecurringRetrievalMode recurRetrieval, final boolean freeBusy)
			throws CalFacadeException {
		return events.getEvents(calendars, filter, startDate, endDate,
				retrieveList, recurRetrieval, freeBusy);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.CoreEventsI#getEvent(java.lang.String,
	 * java.lang.String, java.lang.String, boolean,
	 * org.bedework.calfacade.RecurringRetrievalMode)
	 */
	public Collection<CoreEventInfo> getEvent(final String colPath,
			final String guid, final String rid, final boolean scheduling,
			final RecurringRetrievalMode recurRetrieval)
			throws CalFacadeException {
		checkOpen();
		return events.getEvent(colPath, guid, rid, scheduling, recurRetrieval);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CoreEventsI#addEvent(org.bedework.calfacade.BwEvent
	 * , java.util.Collection, boolean, boolean)
	 */
	public UpdateEventResult addEvent(final BwEvent val,
			final Collection<BwEventProxy> overrides, final boolean scheduling,
			final boolean rollbackOnError) throws CalFacadeException {
		checkOpen();
		UpdateEventResult uer = events.addEvent(val, overrides, scheduling,
				rollbackOnError);

		calendars.touchCalendar(val.getColPath());

		return uer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.CoreEventsI#updateEvent(org.bedework.calfacade.
	 * BwEvent, java.util.Collection, java.util.Collection,
	 * org.bedework.calfacade.util.ChangeTable)
	 */
	public UpdateEventResult updateEvent(final BwEvent val,
			final Collection<BwEventProxy> overrides,
			final Collection<BwEventProxy> deletedOverrides,
			final ChangeTable changes) throws CalFacadeException {
		checkOpen();
		UpdateEventResult ue = null;

		try {
			ue = events.updateEvent(val, overrides, deletedOverrides, changes);
		} catch (CalFacadeException cfe) {
			rollbackTransaction();
			throw cfe;
		}

		calendars.touchCalendar(val.getColPath());

		return ue;
	}

	public DelEventResult deleteEvent(final BwEvent val,
			final boolean scheduling, final boolean reallyDelete)
			throws CalFacadeException {
		checkOpen();
		String colPath = val.getColPath();
		try {
			try {
				return events.deleteEvent(val, scheduling, reallyDelete);
			} catch (CalFacadeException cfe) {
				rollbackTransaction();
				throw cfe;
			}
		} finally {
			calendars.touchCalendar(colPath);
		}
	}

	public void moveEvent(final BwEvent val, final String from, final String to)
			throws CalFacadeException {
		checkOpen();
		events.moveEvent(val, from, to);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bedework.calcorei.EventsI#getEventKeys()
	 */
	public Collection<? extends InternalEventKey> getEventKeysForTzupdate(
			final String lastmod) throws CalFacadeException {
		return events.getEventKeysForTzupdate(lastmod);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.EventsI#getEvent(org.bedework.calcorei.EventsI.
	 * InternalEventKey)
	 */
	public CoreEventInfo getEvent(final InternalEventKey key)
			throws CalFacadeException {
		// Only for super user?

		if (!getSuperUser()) {
			throw new CalFacadeAccessException();
		}

		return events.getEvent(key);
	}

	public Set<CoreEventInfo> getSynchEvents(final String path,
			final String lastmod) throws CalFacadeException {
		return events.getSynchEvents(path, lastmod);
	}

	public CoreEventInfo getEvent(final String colPath, final String val,
			final RecurringRetrievalMode recurRetrieval)
			throws CalFacadeException {
		checkOpen();
		return events.getEvent(colPath, val, recurRetrieval);
	}

	public Collection<BwCalendar> findCalendars(final String guid,
			final String rid) throws CalFacadeException {
		checkOpen();
		return events.findCalendars(guid, rid);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bedework.calcorei.EventsI#getDeletedProxies(org.bedework.calfacade
	 * .BwCalendar)
	 */
	public Collection<CoreEventInfo> getDeletedProxies(final BwCalendar cal)
			throws CalFacadeException {
		return events.getDeletedProxies(cal);
	}

	/*
	 * ====================================================================
	 * Private methods
	 * ====================================================================
	 */

	private SessionFactory getSessionFactory() throws CalFacadeException {
		if (sessionFactory != null) {
			return sessionFactory;
		}

		synchronized (this) {
			if (sessionFactory != null) {
				return sessionFactory;
			}

			sessionFactory = SessionFactoryUtil.getSessionFactory();

			return sessionFactory;
		}
	}

	private Collection<CoreEventInfo> getFreeBusyEntities(
			final Collection<BwCalendar> cals, final BwDateTime start,
			final BwDateTime end, final boolean ignoreTransparency)
			throws CalFacadeException {
		/* Only events and freebusy for freebusy reports. */
		FilterBase filter = new OrFilter();
		filter.addChild(EntityTypeFilter.eventFilter(null, false));
		filter.addChild(EntityTypeFilter.freebusyFilter(null, false));

		RecurringRetrievalMode rrm = new RecurringRetrievalMode(Rmode.expanded,
				start, end);
		Collection<CoreEventInfo> evs = getEvents(cals, filter, start, end,
				null, rrm, true);

		// Filter out transparent and cancelled events

		Collection<CoreEventInfo> events = new TreeSet<CoreEventInfo>();

		for (CoreEventInfo cei : evs) {
			BwEvent ev = cei.getEvent();

			if (!ignoreTransparency
					&& IcalDefs.transparencyTransparent.equals(ev
							.getTransparency())) {
				// Ignore this one.
				continue;
			}

			if (ev.getSuppressed()
					|| BwEvent.statusCancelled.equals(ev.getStatus())) {
				// Ignore this one.
				continue;
			}

			events.add(cei);
		}

		return events;
	}
}
