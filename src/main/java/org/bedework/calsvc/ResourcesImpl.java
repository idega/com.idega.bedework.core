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
import org.bedework.calfacade.BwResource;
import org.bedework.calfacade.BwResourceContent;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calsvci.ResourcesI;

import java.util.Collection;

/** This acts as an interface to the database for filters.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class ResourcesImpl extends CalSvcDb implements ResourcesI {
  /* Used for resource manipulation */

  /** Constructor
   *
   * @param svci
   * @param principal
   */
  ResourcesImpl(final CalSvc svci,
                final BwPrincipal principal) {
    super(svci, principal);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#save(java.lang.String, org.bedework.calfacade.BwResource)
   */
  public void save(final String path,
                   final BwResource val) throws CalFacadeException {
    try {
      setupSharableEntity(val, getPrincipal().getPrincipalRef());

      BwCalendar coll = getSvc().getCalendarsHandler().get(path);

      if (coll == null) {
        throw new CalFacadeException(CalFacadeException.collectionNotFound, path);
      }

      if ((coll.getCalType() == BwCalendar.calTypeCalendarCollection) ||
          (coll.getCalType() == BwCalendar.calTypeExtSub)) {
        throw new CalFacadeException(CalFacadeException.badRequest, path);
      }

      HibSession sess = getSess();

      BwResource r = fetch(sess, val.getName(), coll);

      if (r != null) {
        throw new CalFacadeException(CalFacadeException.duplicateResource,
                                     val.getName());
      }

      sess.save(val);

      BwResourceContent rc = val.getContent();
      rc.setColPath(val.getColPath());
      rc.setName(val.getName());

      sess.save(rc);
    } catch (CalFacadeException cfe) {
      getSvc().rollbackTransaction();
      throw cfe;
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#get(java.lang.String)
   */
  public BwResource get(final String path) throws CalFacadeException {
    CollectionAndName cn = getCollectionAndName(path);

    return fetch(getSess(), cn.name, cn.coll);
  }

  public void getContent(final BwResource val) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwResourceContent.class.getName());
    sb.append(" as rc where rc.colPath=:path and rc.name=:name");

    sess.createQuery(sb.toString());
    sess.setString("path", val.getColPath());
    sess.setString("name", val.getName());
    sess.cacheableQuery();

    BwResourceContent rc = (BwResourceContent)sess.getUnique();
    if (rc == null) {
      throw new CalFacadeException(CalFacadeException.missingResourceContent);
    }

    val.setContent(rc);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#getAll(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  public Collection<BwResource> getAll(final String path) throws CalFacadeException {
    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwResource.class.getName());
    sb.append(" as r where r.ownerHref=:ownerHref and r.colPath=:path");

    sess.createQuery(sb.toString());
    sess.setString("ownerHref", getEntityOwner(getPrincipal()).getPrincipalRef());
    sess.setString("path", path);
    sess.cacheableQuery();

    return sess.getList();
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#update(org.bedework.calfacade.BwResource)
   */
  public void update(final BwResource val,
                     final boolean updateContent) throws CalFacadeException {
    if (!getSvc().getSuperUser() &&
        !getPrincipal().getPrincipalRef().equals(val.getOwnerHref())) {
      throw new CalFacadeAccessException();
    }

    try {
      HibSession sess = getSess();

      sess.update(val);

      if (updateContent && (val.getContent() != null)) {
        BwResourceContent rc = val.getContent();
        rc.setColPath(val.getColPath());
        rc.setName(val.getName());

        sess.update(rc);
      }
    } catch (CalFacadeException cfe) {
      getSvc().rollbackTransaction();
      throw cfe;
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#delete(java.lang.String)
   */
  public void delete(final String path) throws CalFacadeException {
    HibSession sess = getSess();

    CollectionAndName cn = getCollectionAndName(path);

    BwResource r = fetch(sess, cn.name, cn.coll);

    if (r == null) {
      throw new CalFacadeException(CalFacadeException.unknownResource, path);
    }

    try {
      getContent(r);
    } catch (CalFacadeException cfe) {
      if (cfe.getMessage().equals(CalFacadeException.missingResourceContent)) {
        // Swallow it
      } else {
        getSvc().rollbackTransaction();
        throw cfe;
      }
    }

    BwResourceContent rc = r.getContent();

    sess.delete(r);

    if (rc != null) {
      sess.delete(rc);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ResourcesI#copyMove(org.bedework.calfacade.BwResource, java.lang.String, java.lang.String, boolean, boolean)
   */
  public boolean copyMove(final BwResource val,
                          final String to,
                          final String name,
                          final boolean copy,
                          final boolean overwrite) throws CalFacadeException {
    try {
      setupSharableEntity(val, getPrincipal().getPrincipalRef());

      BwCalendar collTo = getSvc().getCalendarsHandler().get(to);

      if (collTo == null) {
        throw new CalFacadeException(CalFacadeException.collectionNotFound, to);
      }

      if (collTo.getCalType() == BwCalendar.calTypeCalendarCollection) {
        // Not allowed into a calendar collection.
        throw new CalFacadeException(CalFacadeException.badRequest, to);
      }

      HibSession sess = getSess();

      BwResource r = fetch(sess, val.getName(), collTo);
      boolean createdNew = false;

      getContent(val);

      if (r != null) {
        /* Update of the target from the source */
        if (!overwrite) {
          throw new CalFacadeException(CalFacadeException.targetExists,
                                       val.getName());
        }

        getContent(r);
        r.setContentType(val.getContentType());

        BwResourceContent rc = r.getContent();
        BwResourceContent toRc = val.getContent();

        r.setContentLength(toRc.getValue().length());

        rc.setValue(val.getContent().getValue());

        sess.update(r);
        sess.update(rc);
      } else {
        /* Create a new resource */

        r = new BwResource();

        setupSharableEntity(r, getPrincipal().getPrincipalRef());

        r.setName(name);
        r.setColPath(collTo.getPath());
        r.setContentType(val.getContentType());
        r.setContentLength(val.getContentLength());

        sess.save(r);

        BwResourceContent fromRc = val.getContent();
        BwResourceContent rc = new BwResourceContent();

        rc.setColPath(collTo.getPath());
        rc.setName(val.getName());
        rc.setValue(fromRc.getValue());

        sess.save(rc);

        createdNew = true;
      }

      if (!copy) {
        // Delete the old one

        BwResourceContent rc = val.getContent();

        sess.delete(rc);
        sess.delete(val);
      }

      return createdNew;
    } catch (CalFacadeException cfe) {
      getSvc().rollbackTransaction();
      throw cfe;
    } catch (Throwable t) {
      getSvc().rollbackTransaction();
      throw new CalFacadeException(t);
    }
  }

  /** Allows svc to retrieve the calSuite object used to configure a public
   * client.
   *
   * @param session
   * @param name
   * @param coll
   * @return BwResource object or null
   * @throws CalFacadeException
   */
  private BwResource fetch(final Object session,
                          final String name,
                          final BwCalendar coll) throws CalFacadeException {
    HibSession sess = (HibSession)session;

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwResource.class.getName());
    sb.append(" where ownerHref=:ownerHref and name=:name and colPath=:path");

    sess.createQuery(sb.toString());
    sess.setString("ownerHref", getEntityOwner(getPrincipal()).getPrincipalRef());
    sess.setString("name", name);
    sess.setString("path", coll.getPath());
    sess.cacheableQuery();

    return (BwResource)sess.getUnique();
  }
}
