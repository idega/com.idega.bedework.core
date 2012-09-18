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
import org.bedework.calfacade.BwGroupEntry;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.exc.CalFacadeException;

import java.util.Collection;
import java.util.TreeSet;

/** An implementation of Directories which stores groups in the calendar
 * database. It is assumed a production system will use the ldap implementation
 * or something like it.
 *
 * @author Mike Douglass douglm@rpi.edu
 * @version 1.0
 */
public class GroupsDbImpl extends AbstractDirImpl {
  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#getGroups(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> getGroups(final BwPrincipal val) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getGroups");
    sess.setInt("entId", val.getId());

    /* This is what I want to do but it inserts 'true' or 'false'
    sess.setBool("isgroup", (val instanceof BwGroup));
    */
    if (val instanceof BwGroup) {
      sess.setString("isgroup", "T");
    } else {
      sess.setString("isgroup", "F");
    }

    return new TreeSet<BwGroup>(sess.getList());
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#getAllGroups(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public Collection<BwGroup> getAllGroups(final BwPrincipal val) throws CalFacadeException {
    Collection<BwGroup> groups = getGroups(val);
    Collection<BwGroup> allGroups = new TreeSet<BwGroup>(groups);

    for (BwGroup grp: groups) {
      Collection<BwGroup> gg = getAllGroups(grp);
      if (!gg.isEmpty()) {
        allGroups.addAll(gg);
      }
    }

    return allGroups;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#getGroupMaintOK()
   */
  @Override
  public boolean getGroupMaintOK() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> getAll(final boolean populate) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery("getAllGroups");

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

    sess.namedQuery("getGroupUserMembers");
    sess.setEntity("gr", group);

    Collection ms = sess.getList();

    sess.namedQuery("getGroupGroupMembers");
    sess.setEntity("gr", group);

    ms.addAll(sess.getList());

    group.setGroupMembers(ms);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#addGroup(org.bedework.calfacade.BwGroup)
   */
  @Override
  public void addGroup(final BwGroup group) throws CalFacadeException {
    if (findGroup(group.getAccount()) != null) {
      throw new CalFacadeException(CalFacadeException.duplicateAdminGroup);
    }
    getSess().save(group);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#findGroup(java.lang.String)
   */
  @Override
  public BwGroup findGroup(final String name) throws CalFacadeException {
    HibSession sess = getSess();

    sess.createQuery("from " + BwGroup.class.getName() + " g " +
                     "where g.account = :account");
    sess.setString("account", name);

    return (BwGroup)sess.getUnique();
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#addMember(org.bedework.calfacade.BwGroup, org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public void addMember(final BwGroup group,
                        final BwPrincipal val) throws CalFacadeException {
    BwGroup g = findGroup(group.getAccount());

    if (g == null) {
      throw new CalFacadeException("Group " + group + " does not exist");
    }

    if (!checkPathForSelf(group, val)) {
      throw new CalFacadeException(CalFacadeException.alreadyOnGroupPath);
    }

    g.addGroupMember(val);

    BwGroupEntry ent = new BwGroupEntry();

    ent.setGrp(g);
    ent.setMember(val);

    getSess().save(ent);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#removeMember(org.bedework.calfacade.BwGroup, org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public void removeMember(final BwGroup group,
                           final BwPrincipal val) throws CalFacadeException {
    BwGroup g = findGroup(group.getAccount());
    HibSession sess = getSess();

    if (g == null) {
      throw new CalFacadeException("Group " + group + " does not exist");
    }

    g.removeGroupMember(val);

    //BwAdminGroupEntry ent = new BwAdminGroupEntry();

    //ent.setGrp(ag);
    //ent.setMember(val);

    sess.namedQuery("findGroupEntry");
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

    BwGroupEntry ent = (BwGroupEntry)sess.getUnique();

    if (ent == null) {
      return;
    }

    getSess().delete(ent);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#removeGroup(org.bedework.calfacade.BwGroup)
   */
  @Override
  public void removeGroup(final BwGroup group) throws CalFacadeException {
    // Remove all group members
    HibSession sess = getSess();

    sess.namedQuery("removeAllGroupMembers");
    sess.setEntity("gr", group);
    sess.executeUpdate();

    // Remove from any groups

    sess.namedQuery("removeFromAllGroups");
    sess.setInt("mbrId", group.getId());

    /* This is what I want to do but it inserts 'true' or 'false'
    sess.setBool("isgroup", (val instanceof BwGroup));
    */
    sess.setString("isgroup", "T");
    sess.executeUpdate();

    sess.delete(group);
 }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Groups#updateGroup(org.bedework.calfacade.BwGroup)
   */
  @Override
  public void updateGroup(final BwGroup group) throws CalFacadeException {
    getSess().saveOrUpdate(group);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<BwGroup> findGroupParents(final BwGroup group) throws CalFacadeException {
    HibSession sess = getSess();

    /* Want this
    sess.createQuery("from " + BwAdminGroup.class.getName() + " ag " +
                     "where mbr in elements(ag.groupMembers)");
    sess.setEntity("mbr", val);
    */

    sess.namedQuery("getGroupParents");
    sess.setInt("grpid", group.getId());

    return sess.getList();
  }

  /* ====================================================================
   *  Abstract methods.
   * ==================================================================== */

  @Override
  protected String getConfigName() {
    return "module.dir-config";
  }

  /* ====================================================================
   *  Private methods.
   * ==================================================================== */

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

