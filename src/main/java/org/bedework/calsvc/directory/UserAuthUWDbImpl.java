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

package org.bedework.calsvc.directory;

import org.bedework.calcorei.HibSession;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.BwAuthUser;
import org.bedework.calfacade.svc.UserAuth;
import org.bedework.calfacade.svc.prefs.BwAuthUserPrefs;

import org.apache.log4j.Logger;

import java.util.Collection;

/** Implementation of UserAuth that handles Bedwork DB tables for authorisation.
 *
 * @author Mike Douglass    douglm@rpi.edu
 * @version 1.0
 */
public class UserAuthUWDbImpl implements UserAuth {
  /** Ideally this would trigger the debugging log in the underlying jdbc
   * implementation.
   */
  private boolean debug;

  private transient Logger log;

  protected CallBack cb;

  /** Constructor
   */
  public UserAuthUWDbImpl() {
  }

  /* ====================================================================
   *  The following affect the state of the current user.
   * ==================================================================== */

  public void initialise(final CallBack cb) throws CalFacadeException {
    this.cb = cb;

    debug = getLogger().isDebugEnabled();
  }

  /** ===================================================================
   *  The following should not change the state of the current users
   *  access which is set and retrieved with the above methods.
   *  =================================================================== */

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.UserAuth#getUserMaintOK()
   */
  public boolean getUserMaintOK() {
    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.UserAuth#updateUser(org.bedework.calfacade.svc.BwAuthUser)
   */
  public void updateUser(final BwAuthUser val) throws CalFacadeException {
    if (val.getUsertype() == noPrivileges) {
      getSess().delete(val);

      return;
    }

    HibSession sess = getSess();

    sess.saveOrUpdate(val);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.UserAuth#getUser(java.lang.String)
   */
  public BwAuthUser getUser(final String account) throws CalFacadeException {
    if (debug) {
      trace("getUserEntry for " + account);
    }

    return getUserEntry(cb.getUser(account));
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.UserAuth#getAll()
   */
  public Collection getAll() throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getAllAuthUsers");

    return sess.getList();
  }

  /* ====================================================================
   *                          Protected methods
   * ==================================================================== */

  protected BwAuthUser getUserEntry(final BwUser u) throws CalFacadeException {
    if (debug) {
      trace("getUserEntry for user " + u);
    }

    if (u == null) {
      return null;
    }

    BwAuthUser au = null;

    HibSession sess = getSess();

    Object o = sess.get(BwAuthUser.class, u.getId());
    if (o != null) {
      au = (BwAuthUser)o;
    } else {
      sess.createQuery("from " + BwAuthUser.class.getName() + " as au " +
                       "where au.userHref = :userHref");
      sess.setString("userHref", u.getPrincipalRef());

      au = (BwAuthUser)sess.getUnique();
    }

    if (au == null) {
      // Not an authorised user
      return null;
    }

    BwAuthUserPrefs prefs = au.getPrefs();

    if (prefs == null) {
      prefs = BwAuthUserPrefs.makeAuthUserPrefs();
      au.setPrefs(prefs);
    }

    return au;
  }

  /*  ===================================================================
   *                   Private methods
   *  =================================================================== */

  private HibSession getSess() throws CalFacadeException {
    HibSession sess = (HibSession)cb.getDbSession();
    if (sess == null) {
      throw new CalFacadeException("Null session returned");
    }

    return sess;
  }
  private Logger getLogger() {
    if (log == null) {
      log = Logger.getLogger(this.getClass());
    }

    return log;
  }

  private void trace(final String msg) {
    getLogger().debug("trace: " + msg);
  }
}
