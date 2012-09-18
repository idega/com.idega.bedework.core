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
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.ifs.HostInfo;
import org.bedework.calsvci.HostsI;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/** Handles hosts information.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class Hosts extends CalSvcDb implements HostsI {
  /* The list of hosts we know about */
  private static List<HostInfo> hosts = new ArrayList<HostInfo>();

  private static long maxLife = 1000 * 60 * 5;

  private static volatile long lastRefresh;

  Hosts(final CalSvc svci,
        final BwUser user) {
    super(svci, user);
  }

  @Override
  public HostInfo get(final String name) throws CalFacadeException {
    List<HostInfo> hs = getAll();

    for (HostInfo h: hs) {
      if (name.equals(h.getHostname())) {
        return h;
      }
    }

    return null;
  }

  private static final String getQuery = "from " + HostInfo.class.getName();

  @Override
  public List<HostInfo> getAll() throws CalFacadeException {
    if ((System.currentTimeMillis() - lastRefresh) < maxLife) {
      return hosts;
    }

    synchronized (hosts) {
      HibSession sess = getSess();

      sess.createQuery(getQuery);

      hosts = new ArrayList(sess.getList());

      lastRefresh = System.currentTimeMillis();
    }

    return hosts;
  }

  @Override
  public HostInfo getHostForRecipient(final String val) throws CalFacadeException {
    /* We may have a url with host + port + path etc or an email like address
     *
     * This is all pretty primitive at the moment and implemented for a demo.
     */
    try {
      URI uri = new URI(val);

      String scheme = uri.getScheme();
      String domain = null;

      if ((scheme == null) || ("mailto".equals(scheme.toLowerCase()))) {
        if (val.indexOf("@") > 0) {
          domain = val.substring(val.indexOf("@") + 1);
        }
      } else {
        domain = uri.getHost();
      }

      if (domain == null) {
        throw new CalFacadeException(CalFacadeException.badCalendarUserAddr);
      }

      return findClosest(domain);
    } catch (URISyntaxException use) {
      throw new CalFacadeException(CalFacadeException.badCalendarUserAddr);
    }
  }

  @Override
  public List<HostInfo> find(final String name) throws CalFacadeException {
    List<HostInfo> hs = getAll();

    List<HostInfo> res = new ArrayList<HostInfo>();

    for (HostInfo h: hs) {
      if (name.equals(h.getHostname())) {
        res.add(h);
      }
    }

    return res;
  }

  @Override
  public HostInfo findClosest(final String name) throws CalFacadeException {
    List<HostInfo> hs = getAll();

    int matchLen = 0;
    HostInfo curHi = null;
    int nameLen = name.length();

    for (HostInfo h: hs) {
      String hostname = h.getHostname();
      int len = hostname.length();

      if (name.endsWith(hostname)) {
        if (len == nameLen) {
          curHi = h;
          break;
        }

        if (len > matchLen) {
          matchLen = len;
          curHi = h;
        }
      }
    }

    return curHi;
  }

  @Override
  public FindHostResult findHost(final String host) throws CalFacadeException {
    return null;
  }

  @Override
  public void add(final HostInfo val) throws CalFacadeException {
    getSess().save(val);

    synchronized (hosts) {
      lastRefresh = 0; // force refresh
    }
  }

  @Override
  public void update(final HostInfo val) throws CalFacadeException {
    getSess().update(val);

    synchronized (hosts) {
      lastRefresh = 0; // force refresh
    }
  }

  @Override
  public void delete(final HostInfo val) throws CalFacadeException {
    getSess().delete(val);

    synchronized (hosts) {
      lastRefresh = 0; // force refresh
    }
  }
}
