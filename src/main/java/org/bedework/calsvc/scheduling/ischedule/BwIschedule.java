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
package org.bedework.calsvc.scheduling.ischedule;

import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.ifs.HostInfo;
import org.bedework.calsvci.CalSvcFactoryDefault;
import org.bedework.calsvci.CalSvcI;
import org.bedework.calsvci.CalSvcIPars;
import org.bedework.calsvci.HostsI;

import java.util.ArrayList;
import java.util.List;

/**
 * @author douglm
 *
 */
public class BwIschedule implements BwIscheduleMBean {
  private String account = "admin";

  private HostInfo curHost;

  private CalSvcI svci;

  public String getName() {
    /* This apparently must be the same as the name attribute in the
     * jboss service definition
     */
    return "org.bedework:service=Indexer";
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#isStarted()
   */
  @Override
  public boolean isStarted() {
    return true;//return (processor != null) && processor.isAlive();
  }

  @Override
  public void setAccount(final String val) {
    account = val;
  }

  @Override
  public String getAccount() {
    return account;
  }

  /* =============================== Current host =========================== */

  /** Get the hostname for the current entry.
   *
   * @return hostname
   */
  @Override
  public String getHostname() {
    if (curHost == null) {
      return "No current entry";
    }

    return curHost.getHostname();
  }

  @Override
  public void setPort(final int val) {
    if (curHost == null) {
      return;
    }

    curHost.setPort(val);
  }

  /** Get the port for the current entry.
   *
   * @return port
   */
  @Override
  public int getPort() {
    if (curHost == null) {
      return -1;
    }

    return curHost.getPort();
  }

  /** Set the secure flag for the current entry.
   *
   * @param val
   */
  @Override
  public void setSecure(final boolean val) {
    if (curHost == null) {
      return;
    }

    curHost.setSecure(val);
  }

  /** Get the secure flag for the current entry.
   *
   * @return secure flag
   */
  @Override
  public boolean getSecure() {
    if (curHost == null) {
      return false;
    }

    return curHost.getSecure();
  }

  /** Set the iSchedule url for the current entry.
   *
   * @param val
   */
  @Override
  public void setIScheduleUrl(final String val) {
    if (curHost == null) {
      return;
    }

    curHost.setIScheduleUrl(val);
  }

  /** Get the iSchedule url for the current entry.
   *
   * @return iSchedule url
   */
  @Override
  public String getIScheduleUrl() {
    if (curHost == null) {
      return "No current entry";
    }

    return curHost.getIScheduleUrl();
  }

  /** Set the iSchedule principal for the current entry.
   *
   * @param val
   */
  @Override
  public void setISchedulePrincipal(final String val) {
    if (curHost == null) {
      return;
    }

    curHost.setISchedulePrincipal(val);
  }

  /** Get the iSchedule principal for the current entry.
   *
   * @return iSchedule principal
   */
  @Override
  public String getISchedulePrincipal() {
    if (curHost == null) {
      return "No current entry";
    }

    return curHost.getISchedulePrincipal();
  }

  /** Set the iSchedule pw for the current entry.
   *
   * @param val
   */
  @Override
  public void setISchedulePw(final String val) {
    if (curHost == null) {
      return;
    }

    curHost.setIScheduleCredentials(val);
  }

  /** Get the iSchedule pw for the current entry.
   *
   * @return iSchedule pw
   */
  @Override
  public String getISchedulePw() {
    if (curHost == null) {
      return "No current entry";
    }

    return curHost.getIScheduleCredentials();
  }

  /* ================================= Operations =========================== */

  @Override
  public String getHost(final String hostname) {
    String result = null;

    try {
      HostsI hosts = getSvci().getHostsHandler();

      curHost = hosts.get(hostname);

      result = "fetched OK";
    } catch (Throwable t) {
      result = t.getMessage();
    } finally {
      String r = closeSvci();

      if (r != null) {
        result = r;
      }
    }

    return result;
  }

  @Override
  public String updateHost() {
    String result = null;

    try {
      HostsI hosts = getSvci().getHostsHandler();

      hosts.update(curHost);

      result = "updated OK";
    } catch (Throwable t) {
      result = t.getMessage();
    } finally {
      String r = closeSvci();

      if (r != null) {
        result = r;
      }
    }

    return result;
  }

  @Override
  public String addIscheduleHost(final String hostname,
                               final int port,
                               final boolean secure,
                               final String url,
                               final String principal,
                               final String pw) {
    String result = null;

    try {
      HostsI hosts = getSvci().getHostsHandler();
      HostInfo hi = hosts.get(hostname);

      if (hi == null) {
        hi = new HostInfo();

        hi.setHostname(hostname);
        hi.setPort(port);
        hi.setSecure(secure);
        hi.setIScheduleUrl(url);
        hi.setISchedulePrincipal(principal);
        hi.setIScheduleCredentials(pw);
        hi.setSupportsISchedule(true);

        hosts.add(hi);

        return "added OK";
      }

      if (hi.getSupportsISchedule()) {
        return "already supports ischedule";
      }

      hi.setPort(port);
      hi.setSecure(secure);
      hi.setIScheduleUrl(url);
      hi.setISchedulePrincipal(principal);
      hi.setIScheduleCredentials(pw);
      hi.setSupportsISchedule(true);

      hosts.update(hi);

      result = "updated OK";
    } catch (Throwable t) {
      result = t.getMessage();
    } finally {
      String r = closeSvci();

      if (r != null) {
        result = r;
      }
    }

    return result;
  }

  @Override
  public List<String> listHosts() {
    List<String> hosts = new ArrayList<String>();

    try {
      List<HostInfo> hs = getSvci().getHostsHandler().getAll();

      for (HostInfo h: hs) {
        hosts.add(h.getHostname());
      }
    } catch (Throwable t) {
      hosts.add("Failed to get hosts: " + t.getMessage());
    } finally {
      String res = closeSvci();
      if (res != null) {
        hosts.add(res);
      }
    }

    return hosts;
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#start()
   */
  @Override
  public synchronized void start() {
  }

  /* (non-Javadoc)
   * @see org.bedework.indexer.BwIndexerMBean#stop()
   */
  @Override
  public synchronized void stop() {
  }

  /** Get an svci object and return it. Also embed it in this object.
   *
   * @return svci object
   * @throws CalFacadeException
   */
  private CalSvcI getSvci() throws CalFacadeException {
    if ((svci != null) && svci.isOpen()) {
      return svci;
    }

    boolean publicAdmin = false;

    if (account == null) {
      return null;
    }

    if (svci == null) {
      CalSvcIPars pars = CalSvcIPars.getServicePars(account,
                                                    publicAdmin,   // publicAdmin
                                                    true);   // Allow super user
      svci = new CalSvcFactoryDefault().getSvc(pars);
    }

    svci.open();
    svci.beginTransaction();

    return svci;
  }

  private String closeSvci() {
    String res = null;
    try {
      if ((svci != null) && svci.isOpen()) {
        svci.endTransaction();
      }
    } catch (Throwable t) {
      res = t.getLocalizedMessage();
    } finally {
      try {
        svci.close();
      } catch (Throwable t) {
        if (res != null) {
          res = t.getLocalizedMessage();
        }
      }
    }

    return res;
  }
}
