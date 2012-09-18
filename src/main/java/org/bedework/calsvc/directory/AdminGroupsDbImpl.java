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
import org.bedework.calfacade.BwGroup;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwPrincipalInfo;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.exc.CalFacadeUnimplementedException;
import org.bedework.calfacade.svc.AdminGroups;
import org.bedework.calfacade.svc.BwAdminGroup;
import org.bedework.calfacade.svc.BwAdminGroupEntry;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/** An implementation of AdminGroups which stores the groups in the calendar
 * database.
 *
 * @author Mike Douglass douglm@rpi.edu
 * @version 1.0
 */
public class AdminGroupsDbImpl extends AbstractDirImpl implements AdminGroups {
  /* ====================================================================
   *  Abstract methods.
   * ==================================================================== */

  @Override
  protected String getConfigName() {
    /* Use the same config as the default groups - we're only after principal info
     */
    return "module.dir-config";
  }

  /* ===================================================================
   *  The following should not change the state of the current users
   *  group.
   *  =================================================================== */

  @Override
  public boolean validPrincipal(final String account) throws CalFacadeException {
    // XXX Not sure how we might use this for admin users.
    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getDirInfo(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public BwPrincipalInfo getDirInfo(final BwPrincipal p) throws CalFacadeException {
    /* Was never previously called - getUserInfo is not defined as a query
    HibSession sess = getSess();

    sess.namedQuery("getUserInfo");
    sess.setString("userHref", p.getPrincipalRef());

    return (BwPrincipalInfo)sess.getUnique(); */
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> getGroups(final BwPrincipal val) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getAdminGroups");
    sess.setInt("entId", val.getId());

    /* This is what I want to do but it inserts 'true' or 'false'
    sess.setBool("isgroup", (val instanceof BwGroup));
    */
    if (val instanceof BwGroup) {
      sess.setString("isgroup", "T");
    } else {
      sess.setString("isgroup", "F");
    }

    Set<BwGroup> gs = new TreeSet<BwGroup>(sess.getList());

    if (val instanceof BwUser) {
      /* Event owner for group is implicit member of group. */

      sess.namedQuery("getAdminGroupsByEventOwner");
      sess.setString("ownerHref", val.getPrincipalRef());

      gs.addAll(sess.getList());
    }

    return gs;
  }

  @Override
  public Collection<BwGroup> getAllGroups(final BwPrincipal val) throws CalFacadeException {
    Collection<BwGroup> groups = getGroups(val);
    Collection<BwGroup> allGroups = new TreeSet<BwGroup>(groups);

    for (BwGroup adgrp: groups) {
//      BwGroup grp = new BwGroup(adgrp.getAccount());

      Collection<BwGroup> gg = getAllGroups(adgrp);
      if (!gg.isEmpty()) {
        allGroups.addAll(gg);
      }
    }

    return allGroups;
  }

  /** Show whether user entries can be modified with this
   * class. Some sites may use other mechanisms.
   *
   * @return boolean    true if group maintenance is implemented.
   */
  @Override
  public boolean getGroupMaintOK() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> getAll(final boolean populate) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getAllAdminGroups");

    Collection<BwGroup> gs = sess.getList();

    if (!populate) {
      return gs;
    }

    for (BwGroup grp: gs) {
      getMembers(grp);
    }

    return gs;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void getMembers(final BwGroup group) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getAdminGroupUserMembers");
    sess.setEntity("gr", group);

    Collection<BwPrincipal> ms = new TreeSet<BwPrincipal>();
    ms.addAll(sess.getList());

    sess.namedQuery("getAdminGroupGroupMembers");
    sess.setEntity("gr", group);

    ms.addAll(sess.getList());

    group.setGroupMembers(ms);
  }

  /* ====================================================================
   *  The following are available if group maintenance is on.
   * ==================================================================== */

  @Override
  public void addGroup(final BwGroup group) throws CalFacadeException {
    if (findGroup(group.getAccount()) != null) {
      throw new CalFacadeException(CalFacadeException.duplicateAdminGroup);
    }
    getSess().save(group);
  }

  /** Find a group given its name
   *
   * @param  name             String group name
   * @return AdminGroupVO   group object
   * @exception CalFacadeException If there's a problem
   */
  @Override
  public BwGroup findGroup(final String name) throws CalFacadeException {
    HibSession sess = getSess();

    sess.createQuery("from " + BwAdminGroup.class.getName() + " ag " +
                     "where ag.account = :account");
    sess.setString("account", name);

    return (BwAdminGroup)sess.getUnique();
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.AdminGroups#addMember(org.bedework.calfacade.BwGroup, org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public void addMember(final BwGroup group,
                        final BwPrincipal val) throws CalFacadeException {
    BwGroup ag = findGroup(group.getAccount());

    if (ag == null) {
      throw new CalFacadeException(CalFacadeException.groupNotFound,
                                   group.getAccount());
    }

    /*
    if (val instanceof BwUser) {
      ensureAuthUserExists((BwUser)val);
    } else {
      val = findGroup(val.getAccount());
    }
    */

    /* val must not already be present on any paths to the root.
     * We'll assume the possibility of more than one parent.
     */

    if (!checkPathForSelf(group, val)) {
      throw new CalFacadeException(CalFacadeException.alreadyOnGroupPath);
    }

    ag.addGroupMember(val);

    BwAdminGroupEntry ent = new BwAdminGroupEntry();

    ent.setGrp(ag);
    ent.setMember(val);

    getSess().save(ent);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.AdminGroups#removeMember(org.bedework.calfacade.BwGroup, org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public void removeMember(final BwGroup group,
                           final BwPrincipal val) throws CalFacadeException {
    BwGroup ag = findGroup(group.getAccount());
    HibSession sess = getSess();

    if (ag == null) {
      throw new CalFacadeException(CalFacadeException.groupNotFound,
                                   group.getAccount());
    }

    ag.removeGroupMember(val);

    //BwAdminGroupEntry ent = new BwAdminGroupEntry();

    //ent.setGrp(ag);
    //ent.setMember(val);

    sess.namedQuery("findAdminGroupEntry");
    sess.setEntity("grp", group);
    sess.setInt("mbrId", val.getId());

    /* This is what I want to do but it inserts 'true' or 'false'
    sess.setBool("isgroup", (val instanceof BwGroup));
    */
    if (val instanceof BwGroup) {
      sess.setString("isgroup", "T");
    } else {
      sess.setString("isgroup", "F");
    }

    BwAdminGroupEntry ent = (BwAdminGroupEntry)sess.getUnique();

    if (ent == null) {
      return;
    }

    getSess().delete(ent);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.AdminGroups#removeGroup(org.bedework.calfacade.BwGroup)
   */
  @Override
  public void removeGroup(final BwGroup group) throws CalFacadeException {
    // Remove all references to group members for this group
    HibSession sess = getSess();

    sess.namedQuery("removeAllAdminGroupMemberRefs");
    sess.setEntity("gr", group);
    sess.executeUpdate();

    // Remove from any groups

    sess.namedQuery("removeFromAllAdminGroups");
    sess.setInt("mbrId", group.getId());

    /* This is what I want to do but it inserts 'true' or 'false'
    sess.setBool("isgroup", (val instanceof BwGroup));
    */
    sess.setString("isgroup", "T");
    sess.executeUpdate();

    sess.delete(group);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.AdminGroups#findGroupByEventOwner(org.bedework.calfacade.BwUser)
   */
  @Override
  public BwAdminGroup findGroupByEventOwner(final BwUser owner)
      throws CalFacadeException {
    HibSession sess = getSess();

    sess.createQuery("from " + BwAdminGroup.class.getName() + " ag " +
                     "where ag.ownerHref = :ownerHref");
    sess.setString("ownerHref", owner.getPrincipalRef());

    return (BwAdminGroup)sess.getUnique();
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.svc.AdminGroups#updateGroup(org.bedework.calfacade.svc.BwAdminGroup)
   */
  @Override
  public void updateGroup(BwGroup group) throws CalFacadeException {
    HibSession sess = getSess();

    group = (BwGroup)sess.merge(group);
    sess.saveOrUpdate(group);
  }

  /* Ensure the authorised user exists - create an entry if not
   *
   * @param val      BwUser account
   * /
  private void ensureAuthUserExists(BwUser u) throws CalFacadeException {
    UserAuth uauth = cb.getUserAuth();

    BwAuthUser au = uauth.getUser(u.getAccount());

    if ((au != null) && (au.getUsertype() != UserAuth.noPrivileges)) {
      return;
    }

    au = new BwAuthUser(u,
                        UserAuth.publicEventUser,
                        "",
                        "",
                        "",
                        "",
                        "");
    uauth.updateUser(au);
  }

  / * Ensure the user exists - create an entry if not
   *
   * @param val      account name
   * @return UserVO  retrieved userVO entry
   * /
  private BwUser ensureUserExists(String val) throws CalFacadeException {
    BwUser user = cb.getUser(val);

    if (user != null) {
      return user;
    }

    user = new BwUser(val);
    cb.addUser(user);

    return user;
  }*/

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> findGroupParents(final BwGroup group) throws CalFacadeException {
    HibSession sess = getSess();

    /* Want this
    sess.createQuery("from " + BwAdminGroup.class.getName() + " ag " +
                     "where mbr in elements(ag.groupMembers)");
    sess.setEntity("mbr", val);
    */

    sess.namedQuery("getAdminGroupParents");
    sess.setInt("grpid", group.getId());

    return sess.getList();
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getGroups(java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String>getGroups(final String rootUrl,
                                     final String principalUrl) throws CalFacadeException {
    // Not needed for admin
    throw new CalFacadeUnimplementedException();
  }

  private boolean checkPathForSelf(final BwGroup group,
                                   final BwPrincipal val) throws CalFacadeException {
    if (group.equals(val)) {
      return false;
    }

    /* get all parents of group and try again */


    for (BwGroup g: findGroupParents(group)) {
      if (!checkPathForSelf(g, val)) {
        return false;
      }
    }

    return true;
  }

  private HibSession getSess() throws CalFacadeException {
    return (HibSession)cb.getDbSession();
  }
}
