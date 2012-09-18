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
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.BwAdminGroup;
import org.bedework.calfacade.svc.BwCalSuite;
import org.bedework.calfacade.svc.wrappers.BwCalSuiteWrapper;
import org.bedework.calsvci.CalSuitesI;

import edu.rpi.cmt.access.PrivilegeDefs;
import edu.rpi.cmt.access.Acl.CurrentAccess;

import java.util.Collection;
import java.util.TreeSet;

/** This acts as an interface to the database for calendar uites.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class CalSuites extends CalSvcDb implements CalSuitesI {
  private BwCalSuiteWrapper currentCalSuite;

  /** Constructor
   *
   * @param svci
   * @param user
   */
  CalSuites(final CalSvc svci,
            final BwUser user) {
    super(svci, user);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#add(org.bedework.calfacade.svc.BwCalSuite)
   */
  public BwCalSuiteWrapper add(final BwCalSuite val) throws CalFacadeException {
    BwCalSuite cs = fetch(getSess(), val.getName());

    if (cs != null) {
      throw new CalFacadeException("org.bedework.duplicate.calsuite");
    }

    setupSharableEntity(val, getPrincipal().getPrincipalRef());

    HibSession sess = getSess();

    sess.save(val);

    return wrap(val, false);
  }

  public void set(final BwCalSuiteWrapper val) throws CalFacadeException {
    currentCalSuite = val;
  }

  public BwCalSuiteWrapper get() throws CalFacadeException {
    if (currentCalSuite == null) {
      return null;
    }

    checkCollections(currentCalSuite);

    return currentCalSuite;
  }

  private void checkCollections(final BwCalSuite cs) throws CalFacadeException {
    if ((cs.getSubmissionsRoot() == null) &&
        (cs.getSubmissionsRootPath() != null)) {
      cs.setSubmissionsRoot(
        getSvc().getCalendarsHandler().get(cs.getSubmissionsRootPath()));
    }

    if ((cs.getRootCollection() == null) &&
        (cs.getRootCollectionPath() != null)) {
      cs.setRootCollection(
        getSvc().getCalendarsHandler().get(cs.getRootCollectionPath()));
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#get(java.lang.String)
   */
  public BwCalSuiteWrapper get(final String name) throws CalFacadeException {
    BwCalSuite cs = fetch(getSess(), name);

    if (cs == null) {
      return null;
    }

    checkCollections(cs);

    return wrap(cs, false);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#get(org.bedework.calfacade.svc.BwAdminGroup)
   */
  public BwCalSuiteWrapper get(final BwAdminGroup group)
        throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getCalSuiteByGroup");
    sess.setEntity("group", group);
    sess.cacheableQuery();

    BwCalSuite cs = (BwCalSuite)sess.getUnique();

    if (cs == null) {
      return null;
    }

    checkCollections(cs);

    return wrap(cs, false);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#getAll()
   */
  @SuppressWarnings("unchecked")
  public Collection<BwCalSuite> getAll() throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwCalSuite.class.getName());

    sess.createQuery(sb.toString());

    sess.cacheableQuery();

    Collection<BwCalSuite> css = sess.getList();

    TreeSet<BwCalSuite> retCss = new TreeSet<BwCalSuite>();

    for (BwCalSuite cs: css) {
      checkCollections(cs);

      BwCalSuite w = wrap(cs, true);

      if (w != null) {
        retCss.add(w);
      }
    }

    return retCss;
  }

  private BwCalSuiteWrapper wrap(final BwCalSuite cs,
                                 final boolean alwaysReturn) throws CalFacadeException {
    CurrentAccess ca = checkAccess(cs, PrivilegeDefs.privAny, alwaysReturn);

    if ((ca == null) || !ca.getAccessAllowed()) {
      return null;
    }

    BwCalSuiteWrapper w = new BwCalSuiteWrapper(cs, ca);

    BwAdminGroup agrp = cs.getGroup();
    if (agrp == null) {
      return w;
    }

    BwPrincipal eventsOwner = getSvc().getUsersHandler().getPrincipal(agrp.getOwnerHref());
    if (eventsOwner == null) {
      return w;
    }

    BwCalendar home = getSvc().getCalendarsHandler().getHome(eventsOwner, false);
    if (home == null) {
      return w;
    }

    w.setResourcesHome(home.getPath());

    return w;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#update(org.bedework.calfacade.svc.wrappers.BwCalSuiteWrapper)
   */
  public void update(final BwCalSuiteWrapper val) throws CalFacadeException {
    HibSession sess = getSess();

    sess.update(val.fetchEntity());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSuitesI#delete(org.bedework.calfacade.svc.wrappers.BwCalSuiteWrapper)
   */
  public void delete(final BwCalSuiteWrapper val) throws CalFacadeException {
    HibSession sess = getSess();

    sess.delete(val.fetchEntity());
  }

  /** Allows svc to retrieve the calSuite object used to configure a public
   * client.
   *
   * @param session
   * @param name
   * @return BwCalSuite object or null
   * @throws CalFacadeException
   */
  public static BwCalSuite fetch(final Object session,
                                  final String name) throws CalFacadeException {
    HibSession sess = (HibSession)session;

    sess.namedQuery("getCalSuite");
    sess.setString("name", name);
    sess.cacheableQuery();

    return (BwCalSuite)sess.getUnique();
  }
}

