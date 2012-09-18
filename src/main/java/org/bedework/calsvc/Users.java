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

import org.bedework.calcorei.HibSession;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwGroup;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.DirectoryInfo;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.BwView;
import org.bedework.calfacade.svc.prefs.BwPreferences;
import org.bedework.calsvci.CalSvcI;
import org.bedework.calsvci.UsersI;
import org.bedework.sysevents.NotificationException;
import org.bedework.sysevents.events.SysEvent;

import edu.rpi.cmt.access.Access;
import edu.rpi.cmt.access.Ace;
import edu.rpi.cmt.access.PrivilegeDefs;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** This acts as an interface to the database for user objects.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class Users extends CalSvcDb implements UsersI {
  private String userCalendarRootPath;

  /* The account that owns public entities
   */
  private String publicUserAccount;

  private BwUser publicUser;

  Users(final CalSvc svci,
        final BwUser user) throws CalFacadeException {
    super(svci, user);

    userCalendarRootPath = "/" + getSvc().getSysparsHandler().get().getUserCalendarRoot();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#get(java.lang.String)
   */
  public BwUser getUser(final String account) throws CalFacadeException {
    if (account == null) {
      return null;
    }

    setRoots(getSvc());

    String href = getSvc().getDirectories().makePrincipalUri(account,
                                                             Ace.whoTypeUser);

    return (BwUser)getPrincipal(href);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#getAlways(java.lang.String)
   */
  public BwUser getAlways(String account) throws CalFacadeException {
    if (account == null) {
      // Return guest user
      return new BwUser();
    }

    BwUser u = getUser(account);
    if (u == null) {
      if (account.endsWith("/")) {
        account = account.substring(0, account.length() - 1);
      }

      add(account);
    }

    return getUser(account);
  }

  /* Make this session specific for the moment. We could make it static possibly
   * Also flush every few minutes
   */
  private Map<String, BwPrincipal> principalMap = new HashMap<String, BwPrincipal>();

  private long lastFlush = System.currentTimeMillis();
  private static final long flushInt = 1000 * 30 * 5; // 5 minutes

  private static String principalRoot;
  private static String userPrincipalRoot;
  private static String groupPrincipalRoot;

  //private static int principalRootLen;
  private static int userPrincipalRootLen;
  private static int groupPrincipalRootLen;

  private static void setRoots(final CalSvcI svc) throws CalFacadeException {
    if (principalRoot != null) {
      return;
    }

    DirectoryInfo di =  svc.getDirectories().getDirectoryInfo();
    principalRoot = setRoot(di.getPrincipalRoot());
    userPrincipalRoot = setRoot(di.getUserPrincipalRoot());
    groupPrincipalRoot = setRoot(di.getGroupPrincipalRoot());

    //principalRootLen = principalRoot.length();
    userPrincipalRootLen = userPrincipalRoot.length();
    groupPrincipalRootLen = groupPrincipalRoot.length();
  }

  private static String setRoot(final String val) {
    if (val.endsWith("/")) {
      return val;
    }

    return val + "/";
  }

  private BwPrincipal mappedPrincipal(final String val) {
    long now = System.currentTimeMillis();

    if ((now - lastFlush) > flushInt) {
      principalMap.clear();
      lastFlush = now;
      return null;
    }

    return principalMap.get(val);
  }

  public BwPrincipal getPrincipal(final String val) throws CalFacadeException {
    if (val == null) {
      return null;
    }

    BwPrincipal p = mappedPrincipal(val);

    if (p != null) {
      return p;
    }

    setRoots(getSvc());

    if (!val.startsWith(principalRoot)) {
      return null;
    }

    if (val.startsWith(userPrincipalRoot)) {
      HibSession sess = getSess();

      StringBuilder q = new StringBuilder();

      q.append("from ");
      q.append(BwUser.class.getName());
      q.append(" as u where u.principalRef = :href");
      sess.createQuery(q.toString());

      String trimmed;

      if (val.endsWith("/")) {
        trimmed = val.substring(0, val.length() - 1);
      } else {
        trimmed = val;
      }

      sess.setString("href", trimmed);

      p = (BwPrincipal)sess.getUnique();

      if (p != null) {
        principalMap.put(val, p);
      }

      return p;
    }

    if (val.startsWith(groupPrincipalRoot)) {
      BwGroup g = getSvc().getDirectories().findGroup(val.substring(groupPrincipalRootLen));

      if (g != null) {
        principalMap.put(val, g);
      }

      return g;
    }

    return null;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#add(java.lang.String)
   */
  public void add(final String val) throws CalFacadeException {
    getSvc().addUser(val);
  }

  BwUser initUserObject(final String val) throws CalFacadeException {
    String account = val;
    if (account.endsWith("/")) {
      account = account.substring(0, account.length() - 1);
    }

    setRoots(getSvc());

    BwUser user = new BwUser(account);

    user.setCategoryAccess(Access.getDefaultPersonalAccess());
    user.setLocationAccess(Access.getDefaultPersonalAccess());
    user.setContactAccess(Access.getDefaultPersonalAccess());

    user.setQuota(getSvc().getSysparsHandler().get().getDefaultUserQuota());

    user.setPrincipalRef(setRoot(userPrincipalRoot) + account);

    return user;
  }

  void createUser(final String val) throws CalFacadeException {
    BwUser user = initUserObject(val);

    setRoots(getSvc());

    getSess().save(user);

    getSvc().initPrincipal(user);
    initPrincipal(user, getSvc());

    getSvc().getCal().getSpecialCalendar(user, BwCalendar.calTypeInbox,
                                         true, PrivilegeDefs.privAny);

    getSvc().getCal().getSpecialCalendar(user, BwCalendar.calTypeOutbox,
                                         true, PrivilegeDefs.privAny);

    try {
      getSvc().postNotification(SysEvent.makePrincipalEvent(SysEvent.SysCode.NEW_USER,
                                                            user));
    } catch (NotificationException ne) {
      throw new CalFacadeException(ne);
    }

  }

  public void update(final BwPrincipal principal) throws CalFacadeException {
    getSess().update(principal);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#remove(org.bedework.calfacade.BwUser)
   */
  public void remove(final BwUser user) throws CalFacadeException {
    String userRoot = getUserRootPath(user);

    /* views */

    Collection<BwView> views = getSvc().getViewsHandler().getAll(user);
    for (BwView view: views) {
      getSvc().getViewsHandler().remove(view);
    }

    /* Set default calendar to null so we don't get blocked. */
    BwPreferences prefs = getSvc().getPrefsHandler().get(user);

    if (prefs != null) {
      prefs.setDefaultCalendarPath(null);
      getSvc().getPrefsHandler().update(prefs);
    }

    /* collections and user home */

    BwCalendar home = getSvc().getCalendarsHandler().get(userRoot);
    if (home != null) {
      getSvc().getCalendarsHandler().delete(home, true);
    }

    /* Remove preferences */
    getSvc().getPrefsHandler().delete(prefs);

    getSess().delete(user);
  }

  public Collection<BwUser> getInstanceOwners() throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder q = new StringBuilder();

    q.append("from ");
    q.append(BwUser.class.getName());
    q.append(" as u where u.instanceOwner=true");
    sess.createQuery(q.toString());

    return sess.getList();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#logon(org.bedework.calfacade.BwUser)
   */
  public void logon(final BwUser val) throws CalFacadeException {
    Timestamp now = new Timestamp(System.currentTimeMillis());

    val.setLogon(now);
    val.setLastAccess(now);
    getSess().update(val);
  }

  /*
  public void deleteUser(BwUser user) throws CalFacadeException {
    checkOpen();
    throw new CalFacadeException("Unimplemented");
  }*/

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#getDefaultCalendarPath(org.bedework.calfacade.BwPrincipal)
   */
  public String getDefaultCalendarPath(final BwPrincipal pr) throws CalFacadeException {
    StringBuilder sb = getSbUserRootPath(pr);

    sb.append("/");
    sb.append(getSvc().getSysparsHandler().get().getUserDefaultCalendar());

    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#getUserRootPath(org.bedework.calfacade.BwPrincipal)
   */
  public String getUserRootPath(final BwPrincipal pr) throws CalFacadeException {
    return getSbUserRootPath(pr).toString();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.UsersI#initPrincipal(org.bedework.calfacade.BwPrincipal)
   */
  public void initPrincipal(final BwPrincipal principal) throws CalFacadeException {
    initPrincipal(principal, getSvc());
  }

  private void initPrincipal(final BwPrincipal principal,
                             final CalSvc svc) throws CalFacadeException {
    // Add preferences
    BwPreferences prefs = new BwPreferences();

    prefs.setOwnerHref(principal.getPrincipalRef());
    prefs.setDefaultCalendarPath(getDefaultCalendarPath(principal));

    // Add a default view for the calendar home

    BwView view = new BwView();

    view.setName(svc.getSysparsHandler().get().getDefaultUserViewName());

    // Add default subscription to the user root.
    view.addCollectionPath(svc.getCal().getPrincipalRootPath(principal));

    prefs.addView(view);
    prefs.setPreferredView(view.getName());

    prefs.setPreferredViewPeriod("week");
    prefs.setHour24(svc.getSysparsHandler().get().getDefaultUserHour24());

    prefs.setScheduleAutoRespond(principal.getKind() == Ace.whoTypeResource);

    svc.getPrefsHandler().update(prefs);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvc.CalSvcDb#getPublicUser()
   */
  @Override
  public BwUser getPublicUser() throws CalFacadeException {
    if (publicUser == null) {
      publicUser = getUser(getPublicUserAccount());
    }

    if (publicUser == null) {
      throw new CalFacadeException("No guest user proxy account - expected " + publicUserAccount);
    }

    return publicUser;
  }

  /* ====================================================================
   *                   Private methods
   * ==================================================================== */

  private String getPublicUserAccount() throws CalFacadeException {
    if (publicUserAccount == null) {
      publicUserAccount = getSvc().getSysparsHandler().get().getPublicUser();
    }

    return publicUserAccount;
  }

  private StringBuilder getSbUserRootPath(final BwPrincipal pr) throws CalFacadeException {
    StringBuilder sb = new StringBuilder();

    sb.append(userCalendarRootPath);
    sb.append("/");
    if (pr.getKind() == Ace.whoTypeUser) {
      sb.append(pr.getAccountNoSlash());
    } else {
      // XXX More work here
      sb.append(pr.getPrincipalRef());
    }

    return sb;
  }
}
