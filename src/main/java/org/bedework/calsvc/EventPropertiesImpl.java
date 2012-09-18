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
import org.bedework.calfacade.BwCategory;
import org.bedework.calfacade.BwEventProperty;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwString;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.base.BwShareableDbentity;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calsvci.EventProperties;

import edu.rpi.cmt.access.PrivilegeDefs;

import java.util.Collection;

/** Class which handles manipulation of BwEventProperty subclasses which are
 * treated in the same manner, these being Category, Location and contact.
 *
 * <p>Each has a single field which together with the owner makes a unique
 * key and all operations on those classes are the same.
 *
 * @author Mike Douglass   douglm - rpi.edu
 *
 * @param <T> type of property, Location, contact etc.
 */
public class EventPropertiesImpl <T extends BwEventProperty>
        extends CalSvcDb implements EventProperties<T>, PrivilegeDefs {
  private String keyFieldName;

  private String finderFieldName;

  private String className;

  /* Named query to get refs */
  private String refQuery;

  /* Named query to delete all from prefs */
  private String delPrefQuery;

  private boolean adminCanEditAllPublic;

  /** Constructor
  *
  * @param svci
  * @param user
  */
  public EventPropertiesImpl(final CalSvc svci,
                             final BwUser user) {
    super(svci, user);
  }

  /* (non-Javadoc)
   * @see org.bedework.calcorei.EventProperties#init(org.bedework.calcore.hibernate.CalintfHelper.Callback, org.bedework.calcore.AccessUtil, int, java.lang.String, java.lang.String, java.lang.String, java.lang.String, int, boolean)
   */
  public void init(final String keyFieldName,
                   final String finderFieldName,
                   final String className,
                   final String refQuery,
                   final String delPrefQuery,
                   final boolean adminCanEditAllPublic) {
    this.keyFieldName = keyFieldName;
    this.finderFieldName = finderFieldName;
    this.className = className;
    this.refQuery = refQuery;
    this.delPrefQuery = delPrefQuery;
    this.adminCanEditAllPublic = adminCanEditAllPublic;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#get(java.lang.String, java.lang.String)
   */
  @SuppressWarnings("unchecked")
  public Collection<T> get(final String ownerHref,
                           final String creatorHref) throws CalFacadeException {
    /* Use a report query to try to prevent the appearance of a lot of
       persistent objects we don't need.

       This isn't too good. If we change fields we'll need to change this.
       We could use reflection - we could use persistent objects if it
       doesn't mean the reappearance of the non-unique object problem.
    * /
    StringBuilder qstr = new StringBuilder("select new ");
    qstr.append(className);
    qstr.append("(ent.id, ent.creator, ent.owner, ent.access, ent.publick, " +
                 "ent.address, ent.subaddress, ent.link) ");

    qstr.append("from ");
    */

    HibSession sess = getSess();

    StringBuilder qstr = new StringBuilder("from ");
    qstr.append(className);
    qstr.append(" ent where ");
    if (ownerHref != null) {
      qstr.append(" ent.ownerHref=:ownerHref");
    }

    if (creatorHref != null) {
      if (ownerHref != null) {
        qstr.append(" and ");
      }
      qstr.append(" ent.creatorHref=:creatorHref");
    }

    qstr.append(" order by ent.");
    qstr.append(keyFieldName);

    sess.createQuery(qstr.toString());

    if (ownerHref != null) {
      sess.setString("ownerHref", ownerHref);
    }

    if (creatorHref != null) {
      sess.setString("creatorHref", creatorHref);
    }

    return (Collection<T>)getSvc().checkAccess(sess.getList(), privRead, true);
  }

  public Collection<T> get() throws CalFacadeException {
    BwPrincipal owner;
    if (!isPublicAdmin()) {
      owner = getPrincipal();
    } else {
      owner = getPublicUser();
    }

    return get(owner.getPrincipalRef(), null);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#getEditable()
   */
  public Collection<T> getEditable() throws CalFacadeException {
    if (!isPublicAdmin()) {
      return get(getPrincipal().getPrincipalRef(), null);
    }

    if (isSuper() || adminCanEditAllPublic) {
      return get(getPublicUser().getPrincipalRef(), null);
    }

    return get(getPublicUser().getPrincipalRef(),
               getPrincipal().getPrincipalRef());
  }

  public T get(final String uid) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder qstr = new StringBuilder("from ");
    qstr.append(className);
    qstr.append(" ent where uid=:uid");

    sess.createQuery(qstr.toString());

    sess.setString("uid", uid);

    return check((T)sess.getUnique());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#find(org.bedework.calfacade.BwString, java.lang.String)
   */
  @SuppressWarnings("unchecked")
  public T find(final BwString val,
                final String ownerHref) throws CalFacadeException {
    String oh;
    if (ownerHref == null) {
      oh = getPrincipal().getPrincipalRef();
    } else {
      oh = ownerHref;
    }

    HibSession sess = getSess();

    findQuery(false, val, oh);

    return check((T)sess.getUnique());
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#add(org.bedework.calfacade.BwEventProperty)
   */
  @SuppressWarnings("unchecked")
  public boolean add(final T val) throws CalFacadeException {
    setupSharableEntity(val, getPrincipal().getPrincipalRef());

    updateOK(val);

    if (find(val.getFinderKeyValue(), val.getOwnerHref()) != null) {
      return false;
    }

    if (debug) {
      trace("Add " + val);
    }

    HibSession sess = getSess();

    if ((val.getCreatorHref() == null) ||
        (val.getOwnerHref() == null)) {
      throw new CalFacadeException("Owner and creator must be set");
    }

    sess.save(val);

    findQuery(true, val.getFinderKeyValue(), val.getOwnerHref());

    Collection<Long> counts = sess.getList();
    if (counts.iterator().next() > 1) {
      sess.rollback();
      throw new CalFacadeException("org.bedework.duplicate.object");
    }

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#update(org.bedework.calfacade.BwEventProperty)
   */
  @SuppressWarnings("unchecked")
  public void update(final T val) throws CalFacadeException {
    HibSession sess = getSess();

    if ((val.getCreatorHref() == null) ||
        (val.getOwnerHref() == null)) {
      throw new CalFacadeException("Owner and creator must be set");
    }

    if (check(val) == null) {
      throw new CalFacadeAccessException();
    }

    sess.update(val);

    findQuery(true, val.getFinderKeyValue(), val.getOwnerHref());

    Collection<Long> counts = sess.getList();
    if (counts.iterator().next() > 1) {
      sess.rollback();
      throw new CalFacadeException("org.bedework.duplicate.object");
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#delete(org.bedework.calfacade.BwEventProperty)
   */
  @SuppressWarnings("unchecked")
  public int delete(T val) throws CalFacadeException {
    deleteOK(val);

    val = (T)getSess().merge(val);

    /** Only allow delete if not in use
     */
    if (getRefsCount(val) != 0) {
      return 2;
    }

    /* Remove from preferences */
    getSvc().getPrefsHandler().updateAdminPrefs(true, val);

    HibSession sess = getSess();

    sess.namedQuery(delPrefQuery);
    sess.setInt("id", val.getId());
    sess.executeUpdate();

    sess.delete(val);
    return 0;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#getRefs(org.bedework.calfacade.BwEventProperty)
   */
  public Collection<PropertyRef> getRefs(final T val) throws CalFacadeException {
    Collection<PropertyRef> refs = getRefs(val, refQuery);

    /* The parameterization doesn't quite cut it for categories. They can appear
     * on collections as well
     */
    if (val instanceof BwCategory) {
      refs.addAll(getRefs(val, refQuery + "Col"));
    }

    return refs;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#getRefsCount(org.bedework.calfacade.BwEventProperty)
   */
  public long getRefsCount(final T val) throws CalFacadeException {
    long total = getRefsCount(val, refQuery + "Count");

    /* The parameterization doesn't quite cut it for categories. They can appear
     * on collections as well
     */
    if (val instanceof BwCategory) {
      total += getRefsCount(val, refQuery + "ColCount");
    }

    return total;
  }

  private long getRefsCount(final T val, final String queryName) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery(queryName);
    sess.setEntity("ent", val);

    /* May get multiple counts back for events and annotations. */
    Collection<Long> counts = sess.getList();

    long total = 0;

    if (debug) {
      trace(" ----------- count = " + counts.size());
      if (counts.size() > 0) {
        trace(" ---------- first el class is " + counts.iterator().next().getClass().getName());
      }
    }

    for (Long l: counts) {
      total += l;
    }

    return total;
  }

  private Collection<PropertyRef> getRefs(final T val,
                                          final String queryName) throws CalFacadeException {
    HibSession sess = getSess();

    sess.namedQuery(queryName);
    sess.setEntity("ent", val);

    /* May get multiple counts back for events and annotations. */
    Collection<PropertyRef> refs = sess.getList();

    if (debug) {
      trace(" ----------- count = " + refs.size());
    }

    return refs;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.EventProperties#ensureExists(org.bedework.calfacade.BwEventProperty, java.lang.String)
   */
  public EnsureEntityExistsResult<T> ensureExists(final T val,
                                                  final String ownerHref)
          throws CalFacadeException {
    EnsureEntityExistsResult<T> eeer = new EnsureEntityExistsResult<T>();

    if (!val.unsaved()) {
      // Exists
      eeer.entity = val;
      return eeer;
    }

    String oh;
    if (ownerHref == null) {
      oh = getPrincipal().getPrincipalRef();
    } else {
      oh = ownerHref;
    }

    eeer.entity = find(val.getFinderKeyValue(), oh);

    if (eeer.entity != null) {
      // Exists
      return eeer;
    }

    // doesn't exist at this point, so we add it to db table
    setupSharableEntity(val, ownerHref);
    eeer.added = add(val);
    eeer.entity = val;

    return eeer;
  }

  /* ====================================================================
   *                   Private methods
   * ==================================================================== */

  private T check(final T ent) throws CalFacadeException {
    if (ent == null) {
      return null;
    }

    /*
    if (!getSvc().checkAccess(ent, privRead, true).accessAllowed) {
      return null;
    }
    */

    return ent;
  }

  private void addBwStringKeyTerms(final BwString key, final String keyName,
                                   final StringBuilder sb) throws CalFacadeException {
    sb.append("((ent.");
    sb.append(keyName);
    sb.append(".lang");

    if (key.getLang() == null) {
      sb.append(" is null) and");
    } else {
      sb.append("=:langval) and");
    }

    sb.append("(ent.");
    sb.append(keyName);
    sb.append(".value");

    if (key.getValue() == null) {
      sb.append(" is null)) ");
    } else {
      sb.append("=:val)) ");
    }
  }

  private void addBwStringKeyvals(final BwString key) throws CalFacadeException {
    HibSession sess = getSess();

    if (key.getLang() != null) {
      sess.setString("langval", key.getLang());
    }

    if (key.getValue() != null) {
      sess.setString("val", key.getValue());
    }
  }

  private void findQuery(final boolean count,
                         final BwString val,
                         final String ownerHref) throws CalFacadeException {
    if (val == null) {
      throw new CalFacadeException("Missing key value");
    }

    if (ownerHref == null) {
      throw new CalFacadeException("Missing owner value");
    }

    HibSession sess = getSess();

    StringBuilder qstr = new StringBuilder();
    if (count) {
      qstr.append("select count(*) ");
    }

    qstr.append("from ");
    qstr.append(className);
    qstr.append(" ent where ");
    addBwStringKeyTerms(val, finderFieldName, qstr);
    qstr.append("and ent.ownerHref=:ownerHref");

    sess.createQuery(qstr.toString());

    addBwStringKeyvals(val);

    sess.setString("ownerHref", ownerHref);
  }

  private void deleteOK(final T o) throws CalFacadeException {
    if (o == null) {
      return;
    }

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
      getSvc().checkAccess(o, privUnbind, false);
      return;
    }

    BwShareableDbentity ent = o;

    if (adminCanEditAllPublic || ent.getCreatorHref().equals(getPrincipal())) {
      return;
    }

    throw new CalFacadeAccessException();
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

    if (adminCanEditAllPublic || ent.getCreatorHref().equals(getPrincipal())) {
      return;
    }

    throw new CalFacadeAccessException();
  }
}

