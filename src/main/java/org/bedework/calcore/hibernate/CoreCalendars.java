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
import org.bedework.calcore.AccessUtil.CollectionGetter;
import org.bedework.calcorei.CoreCalendarsI;
import org.bedework.calcorei.HibSession;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwCollectionLastmod;
import org.bedework.calfacade.BwEventObj;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwStats;
import org.bedework.calfacade.BwStats.CacheStats;
import org.bedework.calfacade.BwSystem;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.CalFacadeDefs;
import org.bedework.calfacade.CollectionSynchInfo;
import org.bedework.calfacade.base.BwLastMod;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeBadRequest;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.exc.CalFacadeInvalidSynctoken;
import org.bedework.calfacade.wrappers.CalendarWrapper;
import org.bedework.sysevents.NotificationException;
import org.bedework.sysevents.events.SysEvent;

import edu.rpi.cmt.access.Access;
import edu.rpi.cmt.access.Ace;
import edu.rpi.cmt.access.AceWho;
import edu.rpi.cmt.access.Acl.CurrentAccess;
import edu.rpi.cmt.access.WhoDefs;
import edu.rpi.sss.util.Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Class to encapsulate most of what we do with collections
 *
 * @author douglm
 *
 */
public class CoreCalendars extends CalintfHelperHib
         implements CollectionGetter, CoreCalendarsI {
  private String userCalendarRootPath;
  private String groupCalendarRootPath;

  /**
   * @author douglm
   *
   */
  private static class CollectionCache implements Serializable {
    private static class CacheInfo {
      CalendarWrapper col;
      String token;
      boolean checked;

      CacheInfo(final CalendarWrapper col) {
        setCol(col);
      }

      void setCol(final CalendarWrapper col) {
        this.col = col;
        token = col.getLastmod().getTagValue();
        checked = true;
      }
    }

    private Map<String, CacheInfo> cache = new HashMap<String, CacheInfo>();

    private CoreCalendars cols;

    //BwStats stats;
    CacheStats cs;

    CollectionCache(final CoreCalendars cols,
                    final BwStats stats) {
      //this.stats = stats;
      this.cols = cols;
      cs = stats.getCollectionCacheStats();
    }

    void put(final CalendarWrapper col) {
      CacheInfo ci = cache.get(col.getPath());

      if (ci != null) {
        // A refetch
        ci.setCol(col);

        cs.incRefetches();
      } else {
        ci = new CacheInfo(col);
        cache.put(col.getPath(), ci);

        cs.incCached();
      }
    }

    void remove(final String path) {
      cache.remove(path);
    }

    CalendarWrapper get(final String path) throws CalFacadeException {
      CacheInfo ci = cache.get(path);

      if (ci == null) {
        cs.incMisses();
        return null;
      }

      if (ci.checked) {
        cs.incHits();
        return ci.col;
      }

      CollectionSynchInfo csi = cols.getSynchInfo(path, ci.token);

      if (csi == null) {
        // Collection deleted?
        cs.incMisses();
        return null;
      }

      if (!csi.changed) {
        ci.checked = true;

        cs.incHits();
        return ci.col;
      }

      return null;  // force refetch
    }

    CalendarWrapper get(final String path, final String token) throws CalFacadeException {
      CacheInfo ci = cache.get(path);

      if (ci == null) {
        cs.incMisses();
        return null;
      }

      if (!ci.token.equals(token)) {
        return null;
      }

      cs.incHits();
      return ci.col;
    }

    void flush() {
      for (CacheInfo ci: cache.values()) {
        ci.checked = false;
      }

      cs.incFlushes();
    }

    void clear() {
      cache.clear();

      cs.incFlushes();
    }
  }

  private CollectionCache colCache;

  /** Constructor
   *
   * @param chcb
   * @param cb
   * @param access
   * @param currentMode
   * @param sessionless
   * @throws CalFacadeException
   */
  public CoreCalendars(final CalintfHelperHibCb chcb, final Callback cb,
                       final AccessUtil access,
                       final int currentMode,
                       final boolean sessionless)
                  throws CalFacadeException {
    super(chcb);
    super.init(cb, access, currentMode, sessionless);

    userCalendarRootPath = "/" + getSyspars().getUserCalendarRoot();
    groupCalendarRootPath = userCalendarRootPath + "/" + "groups";

    colCache = new CollectionCache(this, chcb.getStats());
  }

  /* (non-Javadoc)
   * @see org.bedework.calcore.CalintfHelper#startTransaction()
   */
  @Override
  public void startTransaction() throws CalFacadeException {
    colCache.flush();  // Just in case
  }

  /* (non-Javadoc)
   * @see org.bedework.calcore.CalintfHelper#endTransaction()
   */
  @Override
  public void endTransaction() throws CalFacadeException {
    colCache.flush();
  }

  /* ====================================================================
   *                   CalendarsI methods
   * ==================================================================== */

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#getSynchInfo(java.lang.String, java.lang.String)
   */
  @Override
  public CollectionSynchInfo getSynchInfo(final String path,
                                          final String token) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("select lm.timestamp, lm.sequence from ");
    sb.append(BwCollectionLastmod.class.getName());
    sb.append(" lm where path=:path");
    sess.createQuery(sb.toString());

    sess.setString("path", path);
    sess.cacheableQuery();

    Object[] lmfields = (Object[])sess.getUnique();

    if (lmfields == null) {
      return null;
    }

    CollectionSynchInfo csi = new CollectionSynchInfo();

    csi.token = BwLastMod.getTagValue((String)lmfields[0], (Integer)lmfields[1]);

    csi.changed = (token == null) || (!csi.token.equals(token));

    return csi;
  }

  @Override
  public Collection<BwCalendar> getCalendars(final BwCalendar cal) throws CalFacadeException {
    Collection<BwCalendar> ch = getChildren(cal);

    return checkAccess(ch, privAny, true);
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#resolveAlias(org.bedework.calfacade.BwCalendar, boolean, boolean)
   */
  @Override
  public BwCalendar resolveAlias(final BwCalendar val,
                                 final boolean resolveSubAlias,
                                 final boolean freeBusy) throws CalFacadeException {
    if ((val == null) || !val.getInternalAlias()) {
      return val;
    }

    ArrayList<String> pathElements = new ArrayList<String>();
    pathElements.add(val.getPath());

    return resolveAlias(val, resolveSubAlias, freeBusy, pathElements);
  }

  private BwCalendar resolveAlias(final BwCalendar val,
                                  final boolean resolveSubAlias,
                                  final boolean freeBusy,
                                  final ArrayList<String> pathElements) throws CalFacadeException {
    if ((val == null) || !val.getInternalAlias()) {
      return val;
    }

    BwCalendar c = val.getAliasTarget();
    if (c != null) {
      if (!resolveSubAlias) {
        return c;
      }

      return resolveAlias(c, true, freeBusy, pathElements);
    }

    if (val.getDisabled()) {
      return null;
    }

    int desiredAccess = privRead;
    if (freeBusy) {
      desiredAccess = privReadFreeBusy;
    }

    BwCalendar calendar;

    String path = val.getInternalAliasPath();

    if (pathElements.contains(path)) {
      disableAlias(val);
      return null;
    }

    pathElements.add(path);

    //if (debug) {
    //  trace("Search for calendar \"" + path + "\"");
    //}

    try {
      calendar = getCalendar(path, desiredAccess, false);
    } catch (CalFacadeAccessException cfae) {
      calendar = null;
    }

    if (calendar == null) {
      /* Assume deleted - flag in the subscription if it's ours or a temp.
       */
      if ((val.getId() == CalFacadeDefs.unsavedItemKey) ||
          val.getOwnerHref().equals(getUser())) {
        disableAlias(val);
      }
    } else {
      val.setAliasTarget(calendar);
    }

    if (!resolveSubAlias) {
      return calendar;
    }

    return resolveAlias(calendar, true, freeBusy, pathElements);
  }

  private void disableAlias(final BwCalendar val) throws CalFacadeException {
    val.setDisabled(true);
    if (val.getId() != CalFacadeDefs.unsavedItemKey) {
      // Save the state
      val.updateLastmod();
      getSess().update(unwrap(val));
      //touchCalendar(val.getPath());

      notify(SysEvent.SysCode.COLLECTION_UPDATED, val);

      colCache.put((CalendarWrapper)val);
    }
  }

  @Override
  public BwCalendar getCollection(final String path) throws CalFacadeException {
    if (path == null) {
      return null;
    }

    BwCalendar col = colCache.get(path);

    if (col != null) {
      return col;
    }

    HibSession sess = getSess();

    sess.namedQuery("getCalendarByPath");
    sess.setString("path", path);
    sess.cacheableQuery();

    col = (BwCalendar)sess.getUnique();

    if (col == null) {
      if (path.equals("/")) {
        // Make a root collection
        col = new BwCalendar();
        col.setPath("/");

        // Use this for owner/creator
        BwCalendar userRoot = getCollection(userCalendarRootPath);

        if (userRoot == null) {
          return null;
        }

        col.setOwnerHref(userRoot.getOwnerHref());
        col.setCreatorHref(userRoot.getCreatorHref());
        col.setAccess(Access.getDefaultPublicAccess());
      } else {
        return null;
      }
    }

    CalendarWrapper wcol = wrap(col);
    if (wcol != null) {
      colCache.put(wcol);
    }

    return wcol;
  }

  @Override
  public BwCalendar getCalendar(final String path,
                                final int desiredAccess,
                                final boolean alwaysReturnResult) throws CalFacadeException {
    BwCalendar col = getCollection(path);

    col = checkAccess((CalendarWrapper)col, desiredAccess, alwaysReturnResult);

    return col;
  }

  @Override
  public String getDefaultCalendarPath(final BwUser user) throws CalFacadeException {
    StringBuilder sb = getSbPrincipalRootPath(user);

    sb.append("/");
    sb.append(getSyspars().getUserDefaultCalendar());

    return sb.toString();
  }

  @Override
  public String getPrincipalRootPath(final BwPrincipal principal) throws CalFacadeException {
    return getSbPrincipalRootPath(principal).toString();
  }

  @Override
  public GetSpecialCalendarResult getSpecialCalendar(final BwPrincipal owner,
                                                     final int calType,
                                                     final boolean create,
                                                     final int access) throws CalFacadeException {
    String name;
    BwSystem sys = getSyspars();

    if (calType == BwCalendar.calTypeBusy) {
      name = sys.getBusyCalendar();
    } else if (calType == BwCalendar.calTypeDeleted) {
      name = sys.getDeletedCalendar();
    } else if (calType == BwCalendar.calTypeInbox) {
      name = sys.getUserInbox();
    } else if (calType == BwCalendar.calTypeOutbox) {
      name = sys.getUserOutbox();
    } else if (calType == BwCalendar.calTypeTrash) {
      name = sys.getDefaultTrashCalendar();
    } else {
      // Not supported
      return null;
    }

    StringBuilder sb = getSbPrincipalRootPath(owner);

    String pathTo = sb.toString();

    sb.append("/");
    sb.append(name);

    GetSpecialCalendarResult gscr = new GetSpecialCalendarResult();

    BwCalendar userHome = getCalendar(pathTo, access, false);
    if (userHome == null) {
      gscr.noUserHome = true;
      return gscr;
    }

    gscr.cal = getCalendar(sb.toString(), access, false);

    if ((gscr.cal != null) || !create) {
      return gscr;
    }

    /*
    BwCalendar parent = getCalendar(pathTo, privRead);

    if (parent == null) {
      throw new CalFacadeException("org.bedework.calcore.calendars.unabletocreate");
    }
    */

    gscr.cal = new BwCalendar();
    gscr.cal.setName(name);
    gscr.cal.setCreatorHref(owner.getPrincipalRef());
    gscr.cal.setOwnerHref(owner.getPrincipalRef());
    gscr.cal.setCalType(calType);

    /* I think we're allowing privNone here because we don't mind if the
     * calendar gets created even if the caller has no access.
     */
    gscr.cal = add(gscr.cal, pathTo, true, privNone);
    gscr.created = true;

    return gscr;
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#add(org.bedework.calfacade.BwCalendar, java.lang.String)
   */
  @Override
  public BwCalendar add(final BwCalendar val,
                        final String parentPath) throws CalFacadeException {
    return add(val, parentPath, false, privBind);
  }

  @Override
  public void renameCalendar(BwCalendar val,
                             final String newName) throws CalFacadeException {
    colCache.flush();

    /* update will check access
     */

    BwCalendar parent = getCollection(val.getColPath());

    /* Ensure the name isn't reserved and the path is unique */
    checkNewCalendarName(newName, false, parent);

    val = unwrap(val);

    val.setName(newName);
    val.updateLastmod();

    /* This triggers off a cascade of updates down the tree as we are storing the
     * path in the calendar objects. This may be preferable to calculating the
     * path at every access
     */
    updatePaths(val, parent);

    /* Remove any tombstoned collection with the same name */
    removeTombstonedVersion(val);

    // Flush it again
    colCache.flush();
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CalendarsI#moveCalendar(org.bedework.calfacade.BwCalendar, org.bedework.calfacade.BwCalendar)
   */
  @Override
  public void moveCalendar(BwCalendar val,
                           final BwCalendar newParent) throws CalFacadeException {
    colCache.flush();

    /* check access - privbind on new parent privunbind on val?
     */
    access.checkAccess(val, privUnbind, false);
    access.checkAccess(newParent, privBind, false);

    if (newParent.getCalType() != BwCalendar.calTypeFolder) {
      throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
    }

    val = unwrap(val);

    val.setColPath(newParent.getPath());
    val.updateLastmod();

    BwCalendar tombstoned = (BwCalendar)val.clone();

    tombstoned.tombstone();
    getSess().save(tombstoned);

    /* This triggers off a cascade of updates down the tree as we are storing the
     * path in the calendar objects. This may be preferable to calculating the
     * path at every access
     */
    updatePaths(val, newParent);

    /* Remove any tombstoned collection with the same name */
    removeTombstonedVersion(val);

    // Flush it again
    colCache.flush();
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#touchCalendar(java.lang.String)
   */
  @Override
  public void touchCalendar(final String path) throws CalFacadeException {
    BwCalendar col = getCollection(path);
    if (col == null) {
      return;
    }

    // CALWRAPPER - if we're not cloning can we avoid this?
    //val = (BwCalendar)getSess().merge(val);

    //val = (BwCalendar)getSess().merge(val);

    BwLastMod lm = col.getLastmod();
    lm.updateLastmod();

    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("update ");
    sb.append(BwCollectionLastmod.class.getName());
    sb.append(" set timestamp=:timestamp, sequence=:sequence where path=:path");
    sess.createQuery(sb.toString());

    sess.setString("timestamp", lm.getTimestamp());
    sess.setInt("sequence", lm.getSequence());
    sess.setString("path", path);

    sess.executeUpdate();
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CalendarsI#updateCalendar(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public void updateCalendar(final BwCalendar val) throws CalFacadeException {
    access.checkAccess(val, privWriteProperties, false);

    // CALWRAPPER - did I need this?
    //val = (BwCalendar)getSess().merge(val);

    //val = (BwCalendar)getSess().merge(val);
    val.updateLastmod();
    getSess().update(unwrap(val));
    //touchCalendar(val.getPath());

    notify(SysEvent.SysCode.COLLECTION_UPDATED, val);

    colCache.put((CalendarWrapper)val);
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CalendarsI#changeAccess(org.bedework.calfacade.BwCalendar, java.util.Collection)
   */
  @Override
  public void changeAccess(final BwCalendar cal,
                           final Collection<Ace> aces,
                           final boolean replaceAll) throws CalFacadeException {
    HibSession sess = getSess();

    try {
      access.changeAccess(cal, aces, replaceAll);

      // Clear the cache - inheritance makes it difficult to be sure of the effects.
      colCache.clear();
    } catch (CalFacadeException cfe) {
      sess.rollback();
      throw cfe;
    }

    sess.saveOrUpdate(unwrap(cal));

    ((CalendarWrapper)cal).clearCurrentAccess(); // force recheck
    colCache.put((CalendarWrapper)cal);

    notify(SysEvent.SysCode.COLLECTION_UPDATED, cal);
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CalendarsI#defaultAccess(org.bedework.calfacade.BwCalendar, edu.rpi.cmt.access.AceWho)
   */
  @Override
  public void defaultAccess(final BwCalendar cal,
                            final AceWho who) throws CalFacadeException {
    HibSession sess = getSess();

    access.defaultAccess(cal, who);
    sess.saveOrUpdate(unwrap(cal));

    colCache.flush();

    notify(SysEvent.SysCode.COLLECTION_UPDATED, cal);
  }

  @Override
  public boolean deleteCalendar(BwCalendar val,
                                final boolean reallyDelete) throws CalFacadeException {
    colCache.flush();

    HibSession sess = getSess();

    access.checkAccess(val, privUnbind, false);

    String parentPath = val.getColPath();
    if (parentPath == null) {
      throw new CalFacadeException(CalFacadeException.cannotDeleteCalendarRoot);
    }

    /* Ensure the parent exists and we have writeContent on the parent.
     */
    BwCalendar parent = getCalendar(parentPath, privWriteContent, false);
    if (parent == null) {
      throw new CalFacadeException(CalFacadeException.collectionNotFound);
    }

    val = getCalendar(val.getPath(), privUnbind, false);
    if (val == null) {
      throw new CalFacadeException(CalFacadeException.collectionNotFound);
    }

    if (!isEmpty(val)) {
      throw new CalFacadeException(CalFacadeException.collectionNotEmpty);
    }

    /* See if this is a no-op after all. We do this now to ensure the caller
     * really does have access
     */
    if (!reallyDelete && val.getTombstoned()) {
      // Nothing to do
      return true;
    }

    /* Ensure it's not in any (auth)user preferences */

    sess.namedQuery("removeCalendarPrefForAll");
    sess.setInt("id", val.getId());

    sess.executeUpdate();

    String path = val.getPath();
    BwCalendar unwrapped = unwrap(val);

    /* Ensure no tombstoned events or childen */
    removeTombstoned(val.getPath());

    if (reallyDelete) {
      sess.delete(unwrapped);
    } else {
      unwrapped.tombstone();
      sess.update(unwrapped);
    }

    colCache.remove(path);
    touchCalendar(parentPath);

    notify(SysEvent.SysCode.COLLECTION_DELETED, val);

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#isEmpty(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public boolean isEmpty(final BwCalendar val) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("countCalendarEventRefs");
    sess.setString("colPath", val.getPath());

    Long res = (Long)sess.getUnique();

    if (debug) {
      trace(" ----------- count = " + res);
    }

    if ((res != null) && (res.intValue() > 0)) {
      return false;
    }

    sess.namedQuery("countCalendarChildren");
    sess.setString("colPath", val.getPath());

    res = (Long)sess.getUnique();

    if (debug) {
      trace(" ----------- count children = " + res);
    }

    return (res == null) || (res.intValue() == 0);
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#addNewCalendars(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public void addNewCalendars(final BwPrincipal user) throws CalFacadeException {
    HibSession sess = getSess();

    /* Add a user collection to the userCalendarRoot and then a default
       calendar collection. */

    sess.namedQuery("getCalendarByPath");

    String path =  userCalendarRootPath;
    sess.setString("path", path);

    BwCalendar userrootcal = (BwCalendar)sess.getUnique();

    if (userrootcal == null) {
      throw new CalFacadeException("No user root at " + path);
    }

    BwCalendar parentCal = userrootcal;
    BwCalendar usercal = null;

    /* We may have a principal e.g. /principals/resources/vcc311
     * All except the last may exist already.
     */
    String[] upath = user.getAccountSplit();

    for (int i = 0; i < upath.length; i++) {
      String pathSeg = upath[i];

      if ((pathSeg == null) || (pathSeg.length() == 0)) {
        // Leading or double slash - skip it
        continue;
      }

      path += "/" + pathSeg;
      sess.namedQuery("getCalendarByPath");
      sess.setString("path", path);

      usercal = (BwCalendar)sess.getUnique();
      if (i == (upath.length - 1)) {
        if (usercal != null) {
          throw new CalFacadeException("User calendar already exists at " + path);
        }

        /* Create a folder for the user */
        usercal = new BwCalendar();
        usercal.setName(pathSeg);
        usercal.setCreatorHref(user.getPrincipalRef());
        usercal.setOwnerHref(user.getPrincipalRef());
        usercal.setPublick(false);
        usercal.setPath(path);
        usercal.setColPath(parentCal.getPath());

        sess.save(usercal);
      } else if (usercal == null) {
        /* Create a new system owned folder for part of the principal
         * hierarchy
         */
        usercal = new BwCalendar();
        usercal.setName(pathSeg);
        usercal.setCreatorHref(userrootcal.getCreatorHref());
        usercal.setOwnerHref(userrootcal.getOwnerHref());
        usercal.setPublick(false);
        usercal.setPath(path);
        usercal.setColPath(parentCal.getPath());

        sess.save(usercal);
      }

      parentCal = usercal;
    }

    /* Create a default calendar */
    BwCalendar cal = new BwCalendar();
    cal.setName(getSyspars().getUserDefaultCalendar());
    cal.setCreatorHref(user.getPrincipalRef());
    cal.setOwnerHref(user.getPrincipalRef());
    cal.setPublick(false);
    cal.setPath(path + "/" + cal.getName());
    cal.setColPath(usercal.getPath());
    cal.setCalType(BwCalendar.calTypeCalendarCollection);
    cal.setAffectsFreeBusy(true);

    sess.save(cal);

    sess.update(user);
  }

  @Override
  public Set<BwCalendar> getSynchCols(final String path,
                                      final String token) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    if (path == null) {
      sess.rollback();
      throw new CalFacadeBadRequest("Missing path");
    }

    if ((token != null) && (token.length() < 18)) {
      sess.rollback();
      throw new CalFacadeInvalidSynctoken(token);
    }

    sb.append("from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col ");
    sb.append("where col.colPath=:path ");

    if (token != null) {
      sb.append(" and col.lastmod.timestamp>=:lastmod");
      sb.append(" and col.lastmod.sequence>:seq");
    } else {
      // No deleted collections for null sync-token
      sb.append("and (col.filterExpr is null or col.filterExpr <> :tsfilter)");
    }

    sess.createQuery(sb.toString());

    sess.setString("path", fixPath(path));

    if (token != null) {
      sess.setString("lastmod", token.substring(0, 16));
      sess.setInt("seq", Integer.parseInt(token.substring(17), 16));
    } else {
      sess.setString("tsfilter", BwCalendar.tombstonedFilter);
    }

    sess.cacheableQuery();

    @SuppressWarnings("unchecked")
    List<BwCalendar> cols = sess.getList();

    Set<BwCalendar> res = new TreeSet<BwCalendar>();

    for (BwCalendar col: cols) {
      BwCalendar wcol = wrap(col);
      CurrentAccess ca = access.checkAccess(wcol, privAny, true);
      if (!ca.getAccessAllowed()) {
        continue;
      }

      res.add(wcol);
    }

    return res;
  }

  @Override
  public String getSyncToken(final String path) throws CalFacadeException {
    /* If we'd stored the paths with a terminating "/" this could be done with
     * one query - we have to do it with 2.
     */

    String fpath = fixPath(path);
    BwCalendar thisCol = getCalendar(fpath, privAny, false);

    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col ");
    sb.append("where col.path like :path ");

    sess.createQuery(sb.toString());

    sess.setString("path", fpath + "/");

    @SuppressWarnings("unchecked")
    List<BwCalendar> cols = sess.getList();

    String token = thisCol.getLastmod().getTagValue();

    for (BwCalendar col: cols) {
      BwCalendar wcol = wrap(col);
      CurrentAccess ca = access.checkAccess(wcol, privAny, true);
      if (!ca.getAccessAllowed()) {
        continue;
      }

      String t = col.getLastmod().getTagValue();

      if (t.compareTo(token) > 0) {
        token = t;
      }
    }

    return token;
  }

  /* ====================================================================
   *                  Admin support
   * ==================================================================== */

  /* (non-Javadoc)
   * @see org.bedework.calcorei.CoreCalendarsI#getChildren(java.lang.String, int, int)
   */
  @Override
  @SuppressWarnings("unchecked")
  public Collection<String> getChildCollections(final String parentPath,
                                        final int start,
                                        final int count) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder("select col.path from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col where col.colPath");

    if (parentPath == null) {
      sb.append(" is null");
    } else {
      sb.append("=:colPath");
    }

    // XXX tombstone-schema
    sb.append(" and col.filterExpr <> :tsfilter");

    sess.createQuery(sb.toString());

    if (parentPath != null) {
      sess.setString("colPath", parentPath);
    }

    sess.setString("tsfilter", BwCalendar.tombstonedFilter);

    sess.setFirstResult(start);
    sess.setMaxResults(count);

    List res = sess.getList();

    if (Util.isEmpty(res)) {
      return null;
    }

    return res;
  }

  /* ====================================================================
   *                   Private methods
   * ==================================================================== */

  private void removeTombstoned(final String path) throws CalFacadeException {
    HibSession sess = getSess();

    String fpath = fixPath(path);
    StringBuilder sb = new StringBuilder();

    sb.append("delete from ");
    sb.append(BwEventObj.class.getName());
    sb.append(" ev where ev.tombstoned = true and ");

    sb.append("ev.colPath = :path");

    sess.createQuery(sb.toString());

    sess.setString("path", fpath);

    sess.executeUpdate();

    sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col where col.colPath = :path and ");

    // XXX tombstone-schema
    sb.append("col.filterExpr = :tsfilter");

    sess.createQuery(sb.toString());

    sess.setString("path", fpath);
    sess.setString("tsfilter", BwCalendar.tombstonedFilter);

    @SuppressWarnings("unchecked")
    List<BwCalendar> cols = sess.getList();

    if (!Util.isEmpty(cols)) {
      for (BwCalendar col: cols) {
        sess.delete(col);
      }
    }
  }

  private void checkNewCalendarName(final String name,
                                    final boolean special,
                                    final BwCalendar parent) throws CalFacadeException {
    // XXX This should be accessible to all implementations.
    if (!special) {
      BwSystem sys = getSyspars();

      /* Ensure the name isn't reserved */

      if (name.equals(sys.getBusyCalendar())) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }

      if (name.equals(sys.getDeletedCalendar())) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }

      if (name.equals(sys.getUserInbox())) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }

      if (name.equals(sys.getUserOutbox())) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }

      if (name.equals(sys.getDefaultTrashCalendar())) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }
    }

    /* Ensure the name is not-null and contains no invalid characters
     */
    if ((name == null) ||
        name.contains("/")) {
      throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
    }

    /* Ensure the new path is unique */
    String path;
    if (parent == null) {
      path = "";
    } else {
      path = parent.getPath();
    }

    path += "/" + name;
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col where col.colPath = :path");

    sess.createQuery(sb.toString());

    sess.setString("path", path);

    BwCalendar col = (BwCalendar)sess.getUnique();

    if (col != null) {
      if (!col.getTombstoned()) {
        throw new CalFacadeException(CalFacadeException.duplicateCalendar);
      }

      sess.delete(col);
    }
  }

  private StringBuilder getSbPrincipalRootPath(final BwPrincipal owner) throws CalFacadeException {
    StringBuilder sb = new StringBuilder();

    /* For the moment this builds some very odd paths for non-user principals
     */

    if (owner.getKind() == WhoDefs.whoTypeUser) {
      sb.append(userCalendarRootPath);
    } else {
      sb.append(groupCalendarRootPath);
    }
    sb.append("/");
    sb.append(owner.getAccountNoSlash()); // TRAILSLASH

    return sb;
  }

  private BwCalendar add(final BwCalendar val,
                         final String parentPath,
                         final boolean special,
                         final int access) throws CalFacadeException {
    HibSession sess = getSess();

    BwCalendar parent = null;
    String newPath;

    if ("/".equals(parentPath)) {
      // creating a new root
      newPath = "/" + val.getName();
    } else {
      parent = getCalendar(parentPath, access, false);

      if (parent == null) {
        throw new CalFacadeException(CalFacadeException.collectionNotFound,
                                     parentPath);
      }

      /** Is the parent a calendar collection or a resource folder?
       */
      if (parent.getCalendarCollection() ||
          (parent.getCalType() == BwCalendar.calTypeResourceCollection)) {
        if (val.getAlias() ||
            ((val.getCalType() != BwCalendar.calTypeFolder) &&
            (val.getCalType() != BwCalendar.calTypeResourceCollection))) {
          throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
        }

        if (val.getCalType() == BwCalendar.calTypeFolder) {
          val.setCalType(BwCalendar.calTypeResourceCollection);
        }
      } else if (parent.getCalType() != BwCalendar.calTypeFolder) {
        throw new CalFacadeException(CalFacadeException.illegalCalendarCreation);
      }

      newPath = parent.getPath() + "/" + val.getName();
    }

    /* Ensure the name isn't reserved and is unique */
    checkNewCalendarName(val.getName(), special, parent);

    val.setPath(newPath);
    if (val.getOwnerHref() == null) {
      val.setOwnerHref(getUser().getPrincipalRef());
    }
    val.updateLastmod();

    if (parent != null) {
      val.setColPath(parent.getPath());
      val.setPublick(parent.getPublick());
    }

    /* Remove any tombstoned collection with the same name */
    removeTombstonedVersion(val);

    // No cascades - explicitly save child
    sess.save(val);

    if (parent != null) {
      touchCalendar(parent.getPath());
    }

    notify(SysEvent.SysCode.COLLECTION_ADDED, val);

    CalendarWrapper wcol = wrap(val);

    colCache.put(wcol);

    return checkAccess(wcol, privAny, true);
  }

  private void removeTombstonedVersion(final BwCalendar val) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwCalendar.class.getName());
    sb.append(" col ");
    sb.append("where col.path=:path ");

    sess.createQuery(sb.toString());

    sess.setString("path", val.getPath() + BwCalendar.tombstonedSuffix);

    BwCalendar col = (BwCalendar)sess.getUnique();

    if (col != null) {
      sess.delete(col);
    }
  }

  private void updatePaths(BwCalendar val,
                           final BwCalendar newParent) throws CalFacadeException {
    Collection<BwCalendar> children = getChildren(val);

    String oldColPath = val.getColPath();

    val = unwrap(val);

    String ppath = newParent.getPath();
    val.setPath(ppath + "/" + val.getName());
    val.setColPath(ppath);

    val.getLastmod().setPath(val.getPath());
    val.updateLastmod();

    notifyMove(SysEvent.SysCode.COLLECTION_MOVED,
               val.getName(), oldColPath, val);

    //updateCalendar(val);

    for (BwCalendar ch: children) {
      updatePaths(ch, val);
    }
  }

  /** Return a Collection of the objects after checking access and wrapping
   *
   * @param ents          Collection of Bwcalendar
   * @param desiredAccess access we want
   * @param nullForNoAccess boolean flag behaviour on no access
   * @return Collection   of checked objects
   * @throws CalFacadeException for no access or other failure
   */
  private Collection<BwCalendar> checkAccess(final Collection<BwCalendar> ents,
                                             final int desiredAccess,
                                             final boolean nullForNoAccess)
          throws CalFacadeException {
    TreeSet<BwCalendar> out = new TreeSet<BwCalendar>();
    if (ents == null) {
      return out;
    }

    for (BwCalendar cal: ents) {
      cal = checkAccess((CalendarWrapper)cal, desiredAccess, nullForNoAccess);
      if (cal != null) {
        out.add(cal);
      }
    }

    return out;
  }

  private BwCalendar checkAccess(final CalendarWrapper col,
                                 final int desiredAccess,
                                 final boolean alwaysReturnResult)
          throws CalFacadeException {
    if (col == null) {
      return null;
    }

    boolean noAccessNeeded = desiredAccess == privNone;

    CurrentAccess ca = access.checkAccess(col, desiredAccess,
                                          alwaysReturnResult || noAccessNeeded);

    if (!noAccessNeeded && !ca.getAccessAllowed()) {
      return null;
    }

    return col;
  }

  private void notify(final SysEvent.SysCode code,
                      final BwCalendar val) throws CalFacadeException {
    try {
      if (code.equals(SysEvent.SysCode.COLLECTION_DELETED)) {
        postNotification(
           SysEvent.makeCollectionDeletionEvent(code,
                                                val.getPublick(),
                                                val.getOwnerHref(),
                                                val.getColPath()));
      } else {
        postNotification(
           SysEvent.makeCollectionChangeEvent(code, val.getPath()));
      }
    } catch (NotificationException ne) {
      throw new CalFacadeException(ne);
    }
  }

  private void notifyMove(final SysEvent.SysCode code,
                          final String name,
                          final String oldColPath,
                          final BwCalendar val) throws CalFacadeException {
    try {
      postNotification(
         SysEvent.makeCollectionMoveEvent(code, name, oldColPath,
                                          val.getColPath()));
    } catch (NotificationException ne) {
      throw new CalFacadeException(ne);
    }
  }

  /* No access checks performed */
  @SuppressWarnings("unchecked")
  private Collection<BwCalendar> getChildren(final BwCalendar col) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();
    List<BwCalendar> ch;
    List<BwCalendar> wch = new ArrayList<BwCalendar>();

    if (col == null) {
      return wch;
    }

    if (sessionless) {
      /*
         Maybe we should just fetch them. We've probably not seen them and
         we're just working our way down a tree. The 2 phase might be slower.
       */

      sb.append("from ");
      sb.append(BwCalendar.class.getName());
      sb.append(" where colPath=:path");

      // XXX tombstone-schema
      sb.append(" and (filterExpr is null or filterExpr <> :tsfilter)");

      sess.createQuery(sb.toString());

      sess.setString("path", col.getPath());
      sess.setString("tsfilter", BwCalendar.tombstonedFilter);

      ch = sess.getList();
    } else {
      /* Fetch the lastmod and paths of all children then fetch those we haven't
       * got in the cache.
       */

      sb.append("select lm.path, lm.timestamp, lm.sequence from ");
      sb.append(BwCollectionLastmod.class.getName());
      sb.append(" lm, ");
      sb.append(BwCalendar.class.getName());
      sb.append(" col where col.colPath=:path and lm.path=col.path");

      // XXX tombstone-schema
      sb.append(" and (col.filterExpr is null or col.filterExpr <> :tsfilter)");

      sess.createQuery(sb.toString());

      sess.setString("path", col.getPath());
      sess.setString("tsfilter", BwCalendar.tombstonedFilter);
      sess.cacheableQuery();

      List chfields = sess.getList();

      List<String> paths = new ArrayList<String>();

      if (chfields == null) {
        return wch;
      }

      for (Object o: chfields) {
        Object[] fs = (Object[])o;

        String path = (String)fs[0];
        String token = BwLastMod.getTagValue((String)fs[1], (Integer)fs[2]);

        BwCalendar c = colCache.get(path, token);

        if (c != null) {
          wch.add(c);
          continue;
        }

        paths.add(path);
      }

      if (paths.isEmpty()) {
        return wch;
      }

      /* paths lists those we couldn't find in the cache. */

      sb = new StringBuilder();
      sb.append("from ");
      sb.append(BwCalendar.class.getName());
      sb.append(" where path in (:paths)");

      sess.createQuery(sb.toString());

      sess.setParameterList("paths", paths);

      ch = sess.getList();
    }

    /* Wrap the resulting objects. */

    if (ch == null) {
      return wch;
    }

    for (BwCalendar c: ch) {
      CalendarWrapper wc = wrap(c);

      colCache.put(wc);
      wch.add(wc);
    }

    return wch;
  }
}
