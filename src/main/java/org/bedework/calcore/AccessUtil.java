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
package org.bedework.calcore;

import java.util.Collection;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwCategory;
import org.bedework.calfacade.BwContact;
import org.bedework.calfacade.BwLocation;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.base.BwShareableContainedDbentity;
import org.bedework.calfacade.base.BwShareableDbentity;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.util.AccessUtilI;
import org.bedework.calfacade.wrappers.CalendarWrapper;

import edu.rpi.cmt.access.Access;
import edu.rpi.cmt.access.AccessPrincipal;
import edu.rpi.cmt.access.Ace;
import edu.rpi.cmt.access.AceWho;
import edu.rpi.cmt.access.Acl;
import edu.rpi.cmt.access.Acl.CurrentAccess;
import edu.rpi.cmt.access.PrivilegeSet;

/** An access helper class. This class makes some assumptions about the
 * classes it deals with but there are no explicit hibernate, or other
 * persistence engine, dependencies.
 *
 * <p>It assumes access to the parent object when needed,
 * continuing on up to the root. For systems which do not allow for a
 * retrieval of the parent on calls to the getCalendar method, the getParent
 * method for this class will need to be overridden. This would presumably
 * take place within the core implementation.
 *
 * @author Mike Douglass
 */
public class AccessUtil implements AccessUtilI {
  /**
	 * 
	 */
	private static final long serialVersionUID = -6225166055510492705L;

private boolean debug;

  /** For evaluating access control
   */
  private Access access;

  private boolean superUser;

  private BwUser authUser;

  private CallBack cb;

  private String urp;//userRootPath;

  private String uhpp; //userHomePathPrefix;

  /* Null allows all accesses according to user - otherwise restricted to this. */
  private PrivilegeSet maxAllowedPrivs;

  private transient Logger log;

  /* Information created and saved about access on a given path.
   * /
  private class PathInfo implements Serializable {
    String path;   // The key
    Acl pathAcl;   // Merged acl for the path.
    char[] encoded;
  }

  private static class PathInfoMap extends HashMap<String, PathInfo> {
    synchronized PathInfo getInfo(String path) {
      return (PathInfo)get(path);
    }

    synchronized void putInfo(String path, PathInfo pi) {
      put(path, pi);
    }

    synchronized void flush() {
      clear();
    }
  }

  private PathInfoMap pathInfoMap = new PathInfoMap();
  */

  /**
   */
  public interface CollectionGetter {
    /** Get a collection given the path. No access checks are performed.
     *
     * @param  path          String path of calendar
     * @return BwCalendar null for unknown calendar
     * @throws CalFacadeException
     */
    public abstract BwCalendar getCollection(String path) throws CalFacadeException;
  }

  private CollectionGetter cg;

  /**
   * @param cb
   * @throws CalFacadeException
   */
  public void init(final CallBack cb) throws CalFacadeException {
    this.cb = cb;
    debug = getLog().isDebugEnabled();
    try {
      access = new Access();
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /**
   * @param cg
   */
  public void setCollectionGetter(final CollectionGetter cg) {
    this.cg = cg;
  }

  /** Indicate if we are in superuser mode.
   * @param val
   */
  public void setSuperUser(final boolean val) {
    superUser = val;
  }

  /**
   * @return boolean
   */
  public boolean getSuperUser() {
    return superUser;
  }

  /**
   * @param val
   */
  public void setMaximumAllowedPrivs(final PrivilegeSet val) {
    maxAllowedPrivs = val;
  }

  /** Set the current authenticated user.
   *
   * @param val
   */
  public void setAuthUser(final BwUser val) {
    authUser = val;
  }

  /** Called at request start
   *
   */
  public void open() {
  }

  /** Called at request end
   *
   */
  public void close() {
    //pathInfoMap.flush();
  }

  /** Called to get the parent object for a shared entity. This method should be
   * overriden if explicit calls to the back end calendar are required.
   *
   * @param val
   * @return parent calendar or null.
   */
  public BwCalendar getParent(final BwShareableContainedDbentity<?> val)
          throws CalFacadeException {
    return cg.getCollection(val.getColPath());
  }

  /* ====================================================================
   *                   Access control
   * ==================================================================== */

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#getDefaultPublicAccess()
   */
  public String getDefaultPublicAccess() {
    return Access.getDefaultPublicAccess();
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#getDefaultPersonalAccess()
   */
  public String getDefaultPersonalAccess() {
    return Access.getDefaultPersonalAccess();
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#changeAccess(org.bedework.calfacade.base.BwShareableDbentity, java.util.Collection, boolean)
   */
  public void changeAccess(final BwShareableDbentity<?> ent,
                           final Collection<Ace> aces,
                           final boolean replaceAll) throws CalFacadeException {
    try {
      Acl acl = checkAccess(ent, privWriteAcl, false).getAcl();

      Collection<Ace> allAces;
      if (replaceAll) {
        allAces = aces;
      } else {
        allAces = acl.getAces();
        allAces.addAll(aces);
      }


      ent.setAccess(new Acl(allAces).encodeStr());

//      pathInfoMap.flush();
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#defaultAccess(org.bedework.calfacade.base.BwShareableDbentity, edu.rpi.cmt.access.AceWho)
   */
  public void defaultAccess(final BwShareableDbentity<?> ent,
                            final AceWho who) throws CalFacadeException {
    try {
      Acl acl = checkAccess(ent, privWriteAcl, false).getAcl();

      /* Now remove any access */

      if (acl.removeWho(who) != null) {
        ent.setAccess(acl.encodeStr());

//        pathInfoMap.flush();
      }
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#checkAccess(java.util.Collection, int, boolean)
   */
  public Collection<? extends BwShareableDbentity<?>>
                checkAccess(final Collection<? extends BwShareableDbentity<?>> ents,
                                final int desiredAccess,
                                final boolean alwaysReturn)
          throws CalFacadeException {
    TreeSet<BwShareableDbentity<?>> out = new TreeSet<BwShareableDbentity<?>>();

    for (BwShareableDbentity<?> sdbe: ents) {
      if (checkAccess(sdbe, desiredAccess, alwaysReturn).getAccessAllowed()) {
        out.add(sdbe);
      }
    }

    return out;
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.AccessUtilI#checkAccess(org.bedework.calfacade.base.BwShareableDbentity, int, boolean)
   */
  public CurrentAccess checkAccess(final BwShareableDbentity<?> ent,
                                   final int desiredAccess,
                        final boolean alwaysReturnResult) throws CalFacadeException {
    if (ent == null) {
      return null;
    }

    if (ent instanceof CalendarWrapper) {
      CalendarWrapper col = (CalendarWrapper)ent;

      CurrentAccess ca = col.getCurrentAccess(desiredAccess);

      if (ca != null) {
        // Checked already

        if (!ca.getAccessAllowed() && !alwaysReturnResult) {
          throw new CalFacadeAccessException();
        }

        return ca;
      }
    }

    if (debug) {
      String cname = ent.getClass().getName();
      String ident;
      if (ent instanceof BwCalendar) {
        ident = ((BwCalendar)ent).getPath();
      } else {
        ident = String.valueOf(ent.getId());
      }
      getLog().debug("Check access for object " +
                     cname.substring(cname.lastIndexOf(".") + 1) +
                     " ident=" + ident +
                     " desiredAccess = " + desiredAccess);
    }

    try {
      CurrentAccess ca = null;

      AccessPrincipal owner = cb.getPrincipal(ent.getOwnerHref());
      PrivilegeSet maxPrivs = null;

      char[] aclChars = null;

      if (ent instanceof BwCalendar) {
        BwCalendar cal = (BwCalendar)ent;
        String path = cal.getPath();

        /* Special case the access to the user root e.g /user and
         * the 'home' directory, e.g. /user/douglm
         */

        /* I think this was wrong. For superuser we want to see the real
         * access but they are going to be allowed access whatever.
        if (userRootPath.equals(path)) {
          ca = new CurrentAccess();

          if (getSuperUser()) {
            ca.privileges = PrivilegeSet.makeDefaultOwnerPrivileges();
          } else {
            ca.privileges = PrivilegeSet.makeDefaultNonOwnerPrivileges();
          }
        } else if (path.equals(userHomePathPrefix + account)){
          // Accessing user home directory
          if (getSuperUser()) {
            ca = new CurrentAccess();

            ca.privileges = PrivilegeSet.makeDefaultOwnerPrivileges();
          } else {
            // Set the maximumn access
            maxPrivs = PrivilegeSet.userHomeMaxPrivileges;
          }
        }
         */
        if (!getSuperUser()) {
          if (getUserRootPath().equals(path)) {
            ca = new CurrentAccess();

            ca = Acl.defaultNonOwnerAccess;
          } else if (path.equals(getUserHomePathPrefix() + owner.getAccount())){
            // Accessing user home directory
            // Set the maximumn access

            maxPrivs = PrivilegeSet.userHomeMaxPrivileges;
          }
        }
      }

      if (maxPrivs == null) {
        maxPrivs = maxAllowedPrivs;
      } else if (maxAllowedPrivs != null) {
        maxPrivs = PrivilegeSet.filterPrivileges(maxPrivs, maxAllowedPrivs);
      }

      if (ca == null) {
        /* Not special. getAclChars provides merged access for the current
         * entity.
         */
        aclChars = getAclChars(ent);

        if (debug) {
          getLog().debug("aclChars = " + new String(aclChars));
        }

        if (desiredAccess == privAny) {
          ca = access.checkAny(cb, authUser, owner, aclChars, maxPrivs);
        } else if (desiredAccess == privRead) {
          ca = access.checkRead(cb, authUser, owner, aclChars, maxPrivs);
        } else if (desiredAccess == privWrite) {
          ca = access.checkReadWrite(cb, authUser, owner, aclChars, maxPrivs);
        } else {
          ca = access.evaluateAccess(cb, authUser, owner, desiredAccess, aclChars,
                                     maxPrivs);
        }
      }

      if ((authUser != null) && superUser) {
        // Nobody can stop us - BWAAA HAA HAA

        /* Override rather than just create a readable access as code further
         * up expects a valid filled in object.
         */
        if (debug && !ca.getAccessAllowed()) {
          getLog().debug("Override for superuser");
        }
        ca = Acl.forceAccessAllowed(ca);
      }

      if (ent instanceof CalendarWrapper) {
        CalendarWrapper col = (CalendarWrapper)ent;

        col.setCurrentAccess(ca, desiredAccess);
      }

      if (!ca.getAccessAllowed() && !alwaysReturnResult) {
        throw new CalFacadeAccessException();
      }

      return ca;
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* ====================================================================
   *                   Private methods
   * ==================================================================== */

  /* If the entity is not a collection we merge the access in with the container
   * access then return the merged aces. We do this because we call getPathInfo
   * with a collection entity. That method will recurse up to the root.
   *
   * For a calendar we just use the access for the calendar.
   *
   * The calendar/container access might be cached in the pathInfoTable.
   */
  private char[] getAclChars(final BwShareableDbentity<?> ent) throws CalFacadeException {
    if (ent instanceof BwShareableContainedDbentity) {
      BwCalendar container;

      if (ent instanceof BwCalendar) {
        container = (BwCalendar)ent;
      } else {
        container = getParent((BwShareableContainedDbentity<?>)ent);
      }

      String path = container.getPath();

      CalendarWrapper wcol = null;
      if (container instanceof CalendarWrapper) {
    	  	wcol = (CalendarWrapper)container;
      } else if (container instanceof BwCalendar) {
    	  	wcol = new CalendarWrapper(container, this);
      }
      
      

      String aclStr;
      char[] aclChars = null;

      /* Get access for the parent first if we have one */
      BwCalendar parent = getParent(wcol);

      if (parent != null) {
        aclStr = new String(merged(getAclChars(parent),
                                   parent.getPath(),
                                   wcol.getAccess()));
      } else if (wcol.getAccess() != null) {
        aclStr = new String(wcol.getAccess());
      } else {
        // At root
        throw new CalFacadeException("Collections must have default access set at root");
      }

      if (aclStr != null) {
        aclChars = aclStr.toCharArray();
      }

      if (ent instanceof BwCalendar) {
        return aclChars;
      }

      /* Create a merged access string from the entity access and the
       * container access
       */

      return merged(aclChars, path, ent.getAccess());
    }

    /* This is a way of making other objects sort of shareable.
     * The objects are locations, sponsors and categories.
     *
     * We store the default access in the owner principal and manipulate that to give
     * us some degree of sharing.
     *
     * In effect, the owner becomes the container for the object.
     */

    String aclString = null;
    String entAccess = ent.getAccess();
    BwPrincipal owner = (BwPrincipal)cb.getPrincipal(ent.getOwnerHref());

    if (ent instanceof BwCategory) {
      aclString = owner.getCategoryAccess();
    } else if (ent instanceof BwLocation) {
      aclString = owner.getLocationAccess();
    } else if (ent instanceof BwContact) {
      aclString = owner.getContactAccess();
    }

    if (aclString == null) {
      if (entAccess == null) {
        if (ent.getPublick()) {
          return Access.getDefaultPublicAccess().toCharArray();
        }
        return Access.getDefaultPersonalAccess().toCharArray();
      }
      return entAccess.toCharArray();
    }

    if (entAccess == null) {
      return aclString.toCharArray();
    }

    try {
      Acl acl = Acl.decode(entAccess.toCharArray());
      acl = acl.merge(aclString.toCharArray(), "/owner");

      return acl.getEncoded();
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  private char[] merged(final char[] parentAccess,
                        final String path,
                        final String access) throws CalFacadeException {
    try {
      Acl acl = null;

      if (access != null) {
        acl = Acl.decode(access.toCharArray());
      }

      if (acl == null) {
        acl = Acl.decode(parentAccess, path);
      } else {
        acl = acl.merge(parentAccess, path);
      }

      return acl.encodeAll();
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* Create a merged Acl for the given calendar. Progresses up to the root of
   * the system merging acls as it goes.
   * /
  private PathInfo getPathInfo(BwCalendar cal) throws CalFacadeException {
    PathInfo pi = new PathInfo();
    Collection<Ace> aces = new ArrayList<Ace>();

    pi.path = cal.getPath();

    try {
      while (cal != null) {
        String aclString = cal.getAccess();

        if ((aclString != null) && (aclString.length() == 0)) {
          warn("Zero length acl for " + cal.getPath());
          aclString = null;
        }

        /* Validity check. The system MUST be set up so that /public and /user
         * have the appropriate default access stored as the acl string.
         *
         * Failure to do so results in incorrect evaluation of access. We'll
         * check it here.
         * /
        if (aclString == null) {
          if (cal.getColPath() == null) {
            // At root
            throw new CalFacadeException("Calendars must have default access set at root");
          }
        } else if (aces.isEmpty()) {
          aces.addAll(Acl.decode(aclString).getAces());
        } else {
          // leaf acls go last
          Collection<Ace> moreAces = new ArrayList<Ace>();
          moreAces.addAll(Acl.decode(aclString.toCharArray(),
                                     cal.getPath()).getAces());
          moreAces.addAll(aces);

          aces = moreAces;
        }

        cal = getParent(cal);
      }

      pi.pathAcl = new Acl(aces);
      pi.encoded = pi.pathAcl.encodeAll();

      return pi;
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  } */

  private String getUserRootPath() throws CalFacadeException {
    if (urp == null) {
      urp = "/" + cb.getUserCalendarRoot();
    }

    return urp;
  }

  private String getUserHomePathPrefix() throws CalFacadeException {
    if (uhpp == null) {
      uhpp = getUserRootPath() + "/";
    }

    return uhpp;
  }

  /* Update the merged Acl for the given calendar.
   * Doesn't work because any children in the table need the access changing.
  private void updatePathInfo(BwCalendar cal, Acl acl) throws CalFacadeException {
    try {
      String path = cal.getPath();
      PathInfo pi = pathInfoMap.getInfo(path);

      if (pi == null) {
        pi = new PathInfo();

        pi.path = cal.getPath();
      }

      pi.pathAcl = acl;
      pi.encoded = acl.encodeAll();

      pathInfoMap.putInfo(path, pi);
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }
   */

  private Logger getLog() {
    if (log == null) {
      log = Logger.getLogger(getClass());
    }

    return log;
  }

//  private void warn(String msg) {
//    getLog().warn(msg);
//  }
}

