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
import org.bedework.caldav.util.filter.parse.EventQuery;
import org.bedework.calfacade.BwFilterDef;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.filter.SimpleFilterParser.ParseResult;
import org.bedework.calsvci.FiltersI;

import ietf.params.xml.ns.caldav.FilterType;

import java.util.Collection;

/** This acts as an interface to the database for filters.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class Filters extends CalSvcDb implements FiltersI {
  /** Constructor
   *
   * @param svci
   * @param user
   */
  Filters(final CalSvc svci,
          final BwUser user) {
    super(svci, user);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.FiltersI#parse(java.lang.String)
  public BwFilterDef parse(final String val) throws CalFacadeException {
    try {
      BwFilterDef fd = new BwFilterDef();

      fd.setDefinition(val);

      getFilter().parse(val, null);
      fd.setFilters(getFilter().getQuery().filter);

      return fd;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }
   */

  public void parse(final BwFilterDef val) throws CalFacadeException {
    String def = val.getDefinition();

    /* Require xml filters to start with <?xml
     */

    if ((def.length() > 5) && (def.startsWith("<?xml"))) {
      // Assume xml filter

      try {
        FilterType f = org.bedework.caldav.util.filter.parse.Filters.parse(def);
        EventQuery eq = org.bedework.caldav.util.filter.parse.Filters.getQuery(f);
        val.setFilters(eq.filter);
      } catch (Throwable t) {
        throw new CalFacadeException(t);
      }

      return;
    }

    // Assume simple expression filter
    ParseResult pr = getSvc().getFilterParser().parse(def);

    if (pr.ok) {
      val.setFilters(pr.filter);
    } else {
      throw pr.cfe;
    }
  }


  /* (non-Javadoc)
   * @see org.bedework.calsvci.FiltersI#validate(java.lang.String)
   */
  public void validate(final String val) throws CalFacadeException {
    try {
      org.bedework.caldav.util.filter.parse.Filters.parse(val);
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.FiltersI#save(org.bedework.calfacade.BwFilterDef)
   */
  public void save(final BwFilterDef val) throws CalFacadeException {
    setupOwnedEntity(val, getPrincipal().getPrincipalRef());
    validate(val.getDefinition());

    HibSession sess = getSess();

    BwFilterDef fd = fetch(sess, val.getName());

    if (fd != null) {
      throw new CalFacadeException(CalFacadeException.duplicateFilter,
                                   val.getName());
    }

    sess.save(val);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.FiltersI#get(java.lang.String)
   */
  public BwFilterDef get(final String name) throws CalFacadeException {
    return fetch(getSess(), name);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.FiltersI#getAll()
   */
  @SuppressWarnings("unchecked")
  public Collection<BwFilterDef> getAll() throws CalFacadeException {
    BwPrincipal owner = getEntityOwner(getPrincipal()); // This can affect the query if done later

    HibSession sess = getSess();

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwFilterDef.class.getName());
    sb.append(" where ownerHref=:ownerHref");

    sess.createQuery(sb.toString());
    sess.setString("ownerHref", owner.getPrincipalRef());
    sess.cacheableQuery();

    return sess.getList();
  }

  public void update(final BwFilterDef val) throws CalFacadeException {
    if (!getSvc().getSuperUser() && !getPrincipal().equals(val.getOwnerHref())) {
      throw new CalFacadeAccessException();
    }

    HibSession sess = getSess();

    sess.update(val);
  }

  public void delete(final String name) throws CalFacadeException {
    HibSession sess = getSess();

    BwFilterDef fd = fetch(sess, name);

    if (fd == null) {
      throw new CalFacadeException(CalFacadeException.unknownFilter, name);
    }

    sess.delete(fd);
  }

  /** Allows svc to retrieve the calSuite object used to configure a public
   * client.
   *
   * @param session
   * @param name
   * @return BwCalSuite object or null
   * @throws CalFacadeException
   */
  public BwFilterDef fetch(final Object session,
                           final String name) throws CalFacadeException {
    HibSession sess = (HibSession)session;

    StringBuilder sb = new StringBuilder();

    sb.append("from ");
    sb.append(BwFilterDef.class.getName());
    sb.append(" where ownerHref=:ownerHref and name=:name");

    sess.createQuery(sb.toString());
    sess.setString("ownerHref", getEntityOwner(getPrincipal()).getPrincipalRef());
    sess.setString("name", name);
    sess.cacheableQuery();

    return (BwFilterDef)sess.getUnique();
  }
}
