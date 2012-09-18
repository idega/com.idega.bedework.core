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
package org.bedework.calsvc;

import org.bedework.calcorei.Calintf;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.CalFacadeDefs;
import org.bedework.calfacade.RecurringRetrievalMode;
import org.bedework.calfacade.RecurringRetrievalMode.Rmode;
import org.bedework.calfacade.base.BwShareableDbentity;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.EventInfo;
import org.bedework.calfacade.svc.prefs.BwPreferences;
import org.bedework.calsvci.CalendarsI;
import org.bedework.calsvci.SynchI;

import edu.rpi.cmt.access.Acl.CurrentAccess;
import edu.rpi.cmt.access.PrivilegeDefs;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/** This acts as an interface to the database for calendars.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class Calendars extends CalSvcDb implements CalendarsI {
  private String publicCalendarRootPath;
  //private String userCalendarRootPath;

  /** Constructor
   *
   * @param svci
   * @param principal
   * @throws CalFacadeException
   */
  Calendars(final CalSvc svci,
            final BwPrincipal principal) throws CalFacadeException {
    super(svci, principal);

    publicCalendarRootPath = "/" + getSyspars().getPublicCalendarRoot();
    //userCalendarRootPath = "/" + getSyspars().getUserCalendarRoot();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getPublicCalendars()
   */
  @Override
  public BwCalendar getPublicCalendars() throws CalFacadeException {
    return getSvc().getCal().getCalendar(publicCalendarRootPath,
                                         PrivilegeDefs.privRead, true);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getHome()
   */
  @Override
  public BwCalendar getHome() throws CalFacadeException {
    String path;

    if (isGuest() || isPublicAdmin()) {
      path = publicCalendarRootPath;
    } else {
      path = getSvc().getUsersHandler().getUserRootPath(getPrincipal());
    }

    return getSvc().getCal().getCalendar(path, PrivilegeDefs.privRead, true);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getHome(org.bedework.calfacade.BwUser, boolean)
   */
  @Override
  public BwCalendar getHome(final BwPrincipal principal,
                            final boolean freeBusy) throws CalFacadeException {
    int priv;
    if (freeBusy) {
      priv = PrivilegeDefs.privReadFreeBusy;
    } else {
      priv = PrivilegeDefs.privRead;
    }
    return getSvc().getCal().getCalendar(getSvc().getCal().getPrincipalRootPath(principal),
                                         priv, true);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#decomposeVirtualPath(java.lang.String)
   */
  @Override
  public Collection<BwCalendar> decomposeVirtualPath(final String vpath) throws CalFacadeException {
    Collection<BwCalendar> cols = new ArrayList<BwCalendar>();

    /* First see if the vpath is an actual path - generally the case for
     * personal calendar users.
     */

    BwCalendar curCol = get(vpath);

    if ((curCol != null) && !curCol.getAlias()) {
      cols.add(curCol);

      return cols;
    }

    String[] pathEls = normalizeUri(vpath).split("/");

    if ((pathEls == null) || (pathEls.length == 0)) {
      return cols;
    }

    /* First, keep adding elements until we get a BwCalendar result.
     * This handles the user root not being accessible
     */

    curCol = null;
    String startPath = "";
    int pathi = 1;  // Element 0 is a zero length string

    while (pathi < pathEls.length) {
      startPath += "/" + pathEls[pathi];

      pathi++;

      try {
        curCol = get(startPath);
      } catch (CalFacadeAccessException cfae) {
        curCol = null;
      }

      if (curCol != null) {
        // Found the start collection
        if (debug) {
          trace("Start vpath collection:" + curCol.getPath());
        }
        break;
      }
    }

    if (curCol == null) {
      // Bad vpath
      return null;
    }

    buildCollection:
    for (;;) {
      cols.add(curCol);

      if (debug) {
        trace("      vpath collection:" + curCol.getPath());
      }

      // Follow the chain of references for curCol until we reach a non-alias
      if (curCol.getInternalAlias()) {
        BwCalendar nextCol = resolveAlias(curCol, false, false);

        if (nextCol == null) {
          // Bad vpath
          curCol.setDisabled(true);
          curCol.setLastRefreshStatus("400");
          return null;
        }

        curCol = nextCol;

        continue buildCollection;
      }

      /* Not an alias - do we have any more path elements
       */
      if (pathi >= pathEls.length) {
        break buildCollection;
      }

      /* Not an alias and we have more path elements.
       * It should be a collection. Look for the next path
       * element as a child name
       */

      if (curCol.getCalType() != BwCalendar.calTypeFolder) {
        // Bad vpath
        return null;
      }

      /*
      for (BwCalendar col: getChildren(curCol)) {
        if (col.getName().equals(pathEls[pathi])) {
          // Found our child
          pathi++;
          curCol = col;
          continue buildCollection;
        }
      }
      */
      BwCalendar col = get(curCol.getPath() + "/" + pathEls[pathi]);

      if (col == null) {
        /* Child not found - bad vpath */
        return null;
      }

      pathi++;
      curCol = col;
    }

    return cols;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getChildren(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public Collection<BwCalendar> getChildren(final BwCalendar col) throws CalFacadeException {
    if (col.getCalType() == BwCalendar.calTypeAlias) {
      resolveAlias(col, true, false);
    }
    return getSvc().getCal().getCalendars(col.getAliasedEntity());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getAddContentCollections()
   */
  @Override
  public Set<BwCalendar> getAddContentCollections(final boolean includeAliases)
          throws CalFacadeException {
    Set<BwCalendar> cals = new TreeSet<BwCalendar>();

    getAddContentCalendarCollections(includeAliases, getHome(), cals);

    return cals;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#isEmpty(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public boolean isEmpty(final BwCalendar val) throws CalFacadeException {
    return getSvc().getCal().isEmpty(val);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#get(java.lang.String)
   */
  @Override
  public BwCalendar get(String path) throws CalFacadeException{
    if (path == null) {
      return null;
    }

    if ((path.length() > 1) &&
        (path.startsWith(CalFacadeDefs.bwUriPrefix))) {
      path = path.substring(CalFacadeDefs.bwUriPrefix.length());
    }

    if ((path.length() > 1) && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return getSvc().getCal().getCalendar(path, PrivilegeDefs.privAny, false);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getSpecial(int, boolean)
   */
  @Override
  public BwCalendar getSpecial(final int calType,
                               final boolean create) throws CalFacadeException {
    Calintf.GetSpecialCalendarResult gscr =  getSvc().getCal().getSpecialCalendar(
                             getPrincipal(), calType, create,
                                       PrivilegeDefs.privAny);
    if (gscr.noUserHome) {
      getSvc().getUsersHandler().add(getPrincipal().getAccount());
    }

    return getSvc().getCal().getSpecialCalendar(getPrincipal(), calType, create,
                                                PrivilegeDefs.privAny).cal;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#setPreferred(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public void setPreferred(final BwCalendar  val) throws CalFacadeException {
    getSvc().getPrefsHandler().get().setDefaultCalendarPath(val.getPath());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#getPreferred()
   */
  @Override
  public BwCalendar getPreferred() throws CalFacadeException {
    return get(getSvc().getPrefsHandler().get().getDefaultCalendarPath());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#add(org.bedework.calfacade.BwCalendar, java.lang.String)
   */
  @Override
  public BwCalendar add(BwCalendar val,
                        final String parentPath) throws CalFacadeException {
    updateOK(val);

    setupSharableEntity(val, getPrincipal().getPrincipalRef());

    if (val.getPwNeedsEncrypt()) {
      encryptPw(val);
    }

    val =  getSvc().getCal().add(val, parentPath);

    SynchI synch = getSvc().getSynch();

    if (val.getExternalSub()) {
      if (!synch.subscribe(val)) {
        throw new CalFacadeException(CalFacadeException.subscriptionFailed);
      }
    }

    return val;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#rename(org.bedework.calfacade.BwCalendar, java.lang.String)
   */
  @Override
  public void rename(final BwCalendar val,
                     final String newName) throws CalFacadeException {
    getSvc().getCal().renameCalendar(val, newName);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#move(org.bedework.calfacade.BwCalendar, org.bedework.calfacade.BwCalendar)
   */
  @Override
  public void move(final BwCalendar val,
                   final BwCalendar newParent) throws CalFacadeException {
    getSvc().getCal().moveCalendar(val, newParent);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#update(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public void update(final BwCalendar val) throws CalFacadeException {
    /* Ensure it's not in admin prefs if it's a folder.
     * User may have switched from calendar to folder.
     */
    if (!val.getCalendarCollection() && isPublicAdmin()) {
      /* Remove from preferences */
      getSvc().getPrefsHandler().updateAdminPrefs(true, val, null, null, null);
    }

    if (val.getPwNeedsEncrypt()) {
      encryptPw(val);
    }

    getSvc().getCal().updateCalendar(val);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#delete(org.bedework.calfacade.BwCalendar, boolean)
   */
  @Override
  public boolean delete(final BwCalendar val,
                        final boolean emptyIt) throws CalFacadeException {
    return delete(val, emptyIt, false);
  }

  private boolean delete(final BwCalendar val,
                         final boolean emptyIt,
                         final boolean reallyDelete) throws CalFacadeException {
    if (!emptyIt) {
      /** Only allow delete if not in use
       */
      if (!getSvc().getCal().isEmpty(val)) {
        throw new CalFacadeException(CalFacadeException.collectionNotEmpty);
      }
    }

    BwPreferences prefs = getSvc().getPrefsHandler().get(
             getSvc().getUsersHandler().getPrincipal(val.getOwnerHref()));
    if (val.getPath().equals(prefs.getDefaultCalendarPath())) {
      throw new CalFacadeException(CalFacadeException.cannotDeleteDefaultCalendar);
    }

    /* Remove from preferences */
    getSvc().getPrefsHandler().updateAdminPrefs(true, val, null, null, null);

    /* If it' an alias we just delete it - otherwise we might need to empty it.
     */
    if (!val.getInternalAlias() && emptyIt) {
      if (val.getCalendarCollection()) {
        RecurringRetrievalMode rrm = new RecurringRetrievalMode(Rmode.overrides);

        for (EventInfo ei: getSvc().getEventsHandler().getEvents(val,
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null, // retrieveList
                                                                 rrm)) {
          ((Events)getSvc().getEventsHandler()).delete(ei, false, true, true);
        }
      }

      for (BwCalendar cal: getChildren(val)) {
        if (!delete(cal, true, true)) {
          // Somebody else at it
          getSvc().rollbackTransaction();
          throw new CalFacadeException(CalFacadeException.collectionNotFound,
                                       cal.getPath());
        }
      }
    }

    getSvc().getSynch().unsubscribe(val);

    /* Attempt to tombstone it
     */
    return getSvc().getCal().deleteCalendar(val, false);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#isUserRoot(org.bedework.calfacade.BwCalendar)
   */
  @Override
  public boolean isUserRoot(final BwCalendar cal) throws CalFacadeException {
    if ((cal == null) || (cal.getPath() == null)) {
      return false;
    }

    String[] ss = cal.getPath().split("/");
    int pathLength = ss.length - 1;  // First element is empty string

    return (pathLength == 2) &&
           (ss[1].equals(getSvc().getSysparsHandler().get().getUserCalendarRoot()));
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalendarsI#resolveAlias(org.bedework.calfacade.BwCalendar, boolean, boolean)
   */
  @Override
  public BwCalendar resolveAlias(final BwCalendar val,
                                 final boolean resolveSubAlias,
                                 final boolean freeBusy) throws CalFacadeException {
    return getCal().resolveAlias(val, resolveSubAlias, freeBusy);
  }

  @Override
  public CheckSubscriptionResult checkSubscription(final String path) throws CalFacadeException {
    return getSvc().getSynch().checkSubscription(get(path));
  }

  @Override
  public String getSyncToken(final String path) throws CalFacadeException {
    return getCal().getSyncToken(path);
  }

  /* ====================================================================
   *                   package private methods
   * ==================================================================== */

  Set<BwCalendar> getSynchCols(final String path,
                               final String lastmod) throws CalFacadeException {
    return getCal().getSynchCols(path, lastmod);
  }

  BwCalendar getSpecial(final BwPrincipal owner,
                        final int calType,
                        final boolean create,
                        final int access) throws CalFacadeException {
    Calintf.GetSpecialCalendarResult gscr =  getSvc().getCal().getSpecialCalendar(
                                      owner, calType, create,
                                                PrivilegeDefs.privAny);
    if (gscr.noUserHome) {
      getSvc().getUsersHandler().add(owner.getAccount());
    }

    return getSvc().getCal().getSpecialCalendar(owner, calType, create,
                                         PrivilegeDefs.privAny).cal;
  }

  /* ====================================================================
   *                   private methods
   * ==================================================================== */

  private void getAddContentCalendarCollections(final boolean includeAliases,
                                                final BwCalendar root,
                                                final Set<BwCalendar> cals)
        throws CalFacadeException {
    if (!includeAliases && root.getAlias()) {
      return;
    }

    resolveAlias(root, true, false);

    if (root.getCalType() == BwCalendar.calTypeCalendarCollection) {
      /* We might want to add the busy time calendar here -
       * presumably we will want availability stored somewhere.
       * These might be implicit operations however.
       */
      BwCalendar col = root.getAliasedEntity();

      CurrentAccess ca = getSvc().checkAccess(col,
                                              PrivilegeDefs.privWriteContent,
                                              true);

      if (ca.getAccessAllowed()) {
        cals.add(root);  // Might be an alias, might not
      }
      return;
    }

    if (root.getCalendarCollection()) {
      // Leaf but cannot add here
      return;
    }

    for (BwCalendar ch: getChildren(root)) {
      getAddContentCalendarCollections(includeAliases, ch, cals);
    }
  }

  private void encryptPw(final BwCalendar val) throws CalFacadeException {
    try {
      val.setRemotePw(getSvc().getEncrypter().encrypt(val.getRemotePw()));
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* This provides some limits to shareable entity updates for the
   * admin users. It is applied in addition to the normal access checks
   * applied at the lower levels.
   */
  private void updateOK(final Object o) throws CalFacadeException {
    if (isGuest()) {
      throw new CalFacadeAccessException();
    }

    if (isSuper()) {
      // Always ok
      return;
    }

    if (!(o instanceof BwShareableDbentity)) {
      throw new CalFacadeAccessException();
    }

    if (!isPublicAdmin()) {
      // Normal access checks apply
      return;
    }

    BwShareableDbentity ent = (BwShareableDbentity)o;

    if (getPars().getAdminCanEditAllPublicContacts() ||
        ent.getCreatorHref().equals(getPrincipal())) {
      return;
    }

    throw new CalFacadeAccessException();
  }

  private String normalizeUri(String uri) throws CalFacadeException {
    /*Remove all "." and ".." components */
    try {
      uri = new URI(null, null, uri, null).toString();

      uri = new URI(URLEncoder.encode(uri, "UTF-8")).normalize().getPath();

      uri = URLDecoder.decode(uri, "UTF-8");

      if (uri.endsWith("/")) {
        uri = uri.substring(0, uri.length() - 1);
      }

      if (debug) {
        trace("Normalized uri=" + uri);
      }

      return uri;
    } catch (Throwable t) {
      if (debug) {
        error(t);
      }
      throw new CalFacadeException("Bad uri: " + uri);
    }
  }
}
