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

import org.bedework.calfacade.BwGroup;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwPrincipalInfo;
import org.bedework.calfacade.BwPrincipalInfo.BooleanPrincipalProperty;
import org.bedework.calfacade.BwPrincipalInfo.IntPrincipalProperty;
import org.bedework.calfacade.BwProperty;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.DirectoryInfo;
import org.bedework.calfacade.configs.CalAddrPrefixes;
import org.bedework.calfacade.configs.DirConfigProperties;
import org.bedework.calfacade.configs.SystemRoots;
import org.bedework.calfacade.env.CalOptionsFactory;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.ifs.Directories;
import org.bedework.calfacade.svc.prefs.BwPreferences;
import org.bedework.http.client.carddav.CardDavClient;

import edu.rpi.cmt.access.AccessPrincipal;
import edu.rpi.cmt.access.WhoDefs;
import edu.rpi.sss.util.OptionsI;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** A base implementation of Directories which handles some generic directory
 * methods.
 *
 * <p>One of those is to map an apparently flat identifier space onto a
 * principal hierarchy more appropriate to the needs of webdav. For example we
 * might have a user account "jim" or a ticket "TKT12345". These could be mapped
 * on to "/principals/users/jim" and "/principals/tickets/12345".
 *
 * @author Mike Douglass douglm@rpi.edu
 * @version 1.0
 */
public abstract class AbstractDirImpl implements Directories {
  private static SystemRoots sysRoots;
  private static CalAddrPrefixes caPrefixes;

  /**
   * @author douglm
   */
  public static class CAPrefixInfo {
    private String prefix;
    private int type;

    CAPrefixInfo(final String prefix, final int type) {
      this.prefix = prefix;
      this.type = type;
    }

    /**
     * @return prefix - never null
     */
    public String getPrefix() {
      return prefix;
    }

    /**
     * @return type defined in Acl
     */
    public int getType() {
      return type;
    }
  }

  private static Collection<CAPrefixInfo> caPrefixInfo;

  private DirConfigProperties props;

  /** */
  private static class DomainMatcher implements Serializable {
    /* Only simple wildcard matching *, * + chars or chars */
    String pattern;

    boolean exact;

    DomainMatcher(final String pattern) {
      this.pattern = pattern;

      if (!pattern.startsWith("*")) {
        this.pattern = pattern;
        exact = true;
      } else {
        this.pattern = pattern.substring(1);
      }
    }

    boolean matches(final String val, final int atPos) {
      if (atPos < 0) {
        return false;
      }

      int start = atPos + 1;
      int domainLen = val.length() - start;

      if (exact) {
        if (domainLen != pattern.length()) {
          return false;
        }
      } else if (domainLen < pattern.length()) {
        return false;
      }

      return val.endsWith(pattern);
    }
  }

  private DomainMatcher onlyDomain;
  private boolean anyDomain;
  private String defaultDomain;
  private Collection<DomainMatcher> domains;

  protected CallBack cb;

  private transient Logger log;

  private HashMap<String, Integer> toWho = new HashMap<String, Integer>();
  private HashMap<Integer, String> fromWho = new HashMap<Integer, String>();

  protected static class FlushMap<K,V> extends HashMap<K,V> {
    private long lastFlush;
    private long flushTime = 60 * 1000;  // 1 minute

    protected FlushMap(final long flushTime ) {
      this.flushTime = flushTime;
    }

    protected void testFlush() {
      if ((lastFlush != 0) &&
          ((System.currentTimeMillis() - lastFlush) > flushTime)) {
        clear();
      }
    }

    @Override
    public boolean containsKey(final Object key) {
      testFlush();
      return super.containsKey(key);
    }
  }

  private static FlushMap<String, String> validPrincipals =
    new FlushMap<String, String>(60 * 1000 * 5); // 5 minute

  private static FlushMap<String, BwPrincipalInfo> principalInfoMap =
    new FlushMap<String, BwPrincipalInfo>(60 * 1000 * 5); // 5 minute

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#init(org.bedework.calfacade.ifs.Directories.CallBack)
   */
  @Override
  public void init(final CallBack cb) throws CalFacadeException {
    this.cb = cb;

    initWhoMaps(getSystemRoots().getUserPrincipalRoot(), WhoDefs.whoTypeUser);
    initWhoMaps(getSystemRoots().getGroupPrincipalRoot(), WhoDefs.whoTypeGroup);
    initWhoMaps(getSystemRoots().getTicketPrincipalRoot(), WhoDefs.whoTypeTicket);
    initWhoMaps(getSystemRoots().getResourcePrincipalRoot(), WhoDefs.whoTypeResource);
    initWhoMaps(getSystemRoots().getVenuePrincipalRoot(), WhoDefs.whoTypeVenue);
    initWhoMaps(getSystemRoots().getHostPrincipalRoot(), WhoDefs.whoTypeHost);
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getDirectoryInfo()
   */
  @Override
  public DirectoryInfo getDirectoryInfo() throws CalFacadeException {
    DirectoryInfo info = new DirectoryInfo();
    SystemRoots sr = getSystemRoots();

    info.setPrincipalRoot(sr.getPrincipalRoot());
    info.setUserPrincipalRoot(sr.getUserPrincipalRoot());
    info.setGroupPrincipalRoot(sr.getGroupPrincipalRoot());
    info.setBwadmingroupPrincipalRoot(sr.getBwadmingroupPrincipalRoot());
    info.setTicketPrincipalRoot(sr.getTicketPrincipalRoot());
    info.setResourcePrincipalRoot(sr.getResourcePrincipalRoot());
    info.setVenuePrincipalRoot(sr.getVenuePrincipalRoot());
    info.setHostPrincipalRoot(sr.getHostPrincipalRoot());

    return info;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#validPrincipal(java.lang.String)
   */
  @Override
  public synchronized boolean validPrincipal(final String href) throws CalFacadeException {
    // XXX Not sure how we might use this for admin users.
    if (href == null) {
      return false;
    }

    /* Use a map to avoid the lookup if possible.
     * This does mean that we retain traces of a user who gets deleted until
     * we flush.
     */

    if (lookupValidPrincipal(href)) {
      return true;
    }

    boolean valid = !href.startsWith("invalid");  // allow some testing

    try {
      // Is it parseable?
      new URI(href);
    } catch (Throwable t) {
      valid = false;
    }

    if (valid) {
      addValidPrincipal(href);
    }

    return valid;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getDirInfo(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public BwPrincipalInfo getDirInfo(final BwPrincipal p) throws CalFacadeException {
    BwPrincipalInfo pi = principalInfoMap.get(p.getPrincipalRef());

    if (pi != null) {
      return pi;
    }

    // If carddav lookup is enabled - use that

    CardDavInfo cdi = getCardDavInfo(false);

    if ((cdi == null) || (cdi.host == null)) {
      return null;
    }

    CardDavClient cdc = null;

    pi = new BwPrincipalInfo();

    try {
      cdc = new CardDavClient(cdi.host, cdi.port, null, cdi.contextPath,
                              15 * 1000);

      pi.setPropertiesFromVCard(cdc.getCard(p));
    } catch (Throwable t) {
      if (getLogger().isDebugEnabled()) {
        error(t);
      }
    } finally {
      cdc.close();
    }

    principalInfoMap.put(p.getPrincipalRef(), pi);

    return pi;
  }

  @Override
  public boolean mergePreferences(final BwPreferences prefs,
                                  final BwPrincipalInfo pinfo) throws CalFacadeException {
    boolean changed = false;
    //PrincipalProperty kind = pinfo.findProperty("kind");

    /* ============ auto scheduling ================== */
    BooleanPrincipalProperty pautoSched =
      (BooleanPrincipalProperty)pinfo.findProperty("auto-schedule");

    if ((pautoSched != null) &&
        (pautoSched.getVal() != prefs.getScheduleAutoRespond())) {
      prefs.setScheduleAutoRespond(pautoSched.getVal());

      if (pautoSched.getVal()) {
        // Ensure we delete cancelled
        prefs.setScheduleAutoCancelAction(BwPreferences.scheduleAutoCancelDelete);
      }

      changed = true;
    }

    IntPrincipalProperty pschedMaxInstances =
      (IntPrincipalProperty)pinfo.findProperty("max-instances");

    if (pschedMaxInstances != null) {
      int mi = pschedMaxInstances.getVal();
      String strMi = String.valueOf(mi);

      BwProperty pmi = prefs.findProperty(BwPreferences.propertyScheduleMaxinstances);
      if (pmi == null) {
        prefs.addProperty(new BwProperty(BwPreferences.propertyScheduleMaxinstances,
                                         strMi));
      } else if (!pmi.getValue().equals(strMi)) {
        pmi.setValue(strMi);
      }

      changed = true;
    }

    return changed;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#isPrincipal(java.lang.String)
   */
  @Override
  public boolean isPrincipal(final String val) throws CalFacadeException {
    if (val == null) {
      return false;
    }

    /* assuming principal root is "principals" we expect something like
     * "/principals/users/jim".
     *
     * Anything with fewer or greater elements is a collection or entity.
     */

    int pos1 = val.indexOf("/", 1);

    if (pos1 < 0) {
      return false;
    }

    if (!val.substring(0, pos1).equals(getSystemRoots().getPrincipalRoot())) {
      return false;
    }

    int pos2 = val.indexOf("/", pos1 + 1);

    if (pos2 < 0) {
      return false;
    }

    if (val.length() == pos2) {
      // Trailing "/" on 2 elements
      return false;
    }

    for (String root: toWho.keySet()) {
      String pfx = root;
      if (!pfx.endsWith("/")) {
        pfx += "/";
      }

      if (val.startsWith(pfx)) {
        if (val.equals(pfx)) {
          // It IS a root
          return false;
        }
        return true;
      }
    }

    /*
    int pos3 = val.indexOf("/", pos2 + 1);

    if ((pos3 > 0) && (val.length() > pos3 + 1)) {
      // More than 3 elements
      return false;
    }

    if (!toWho.containsKey(val.substring(0, pos2))) {
      return false;
    }
    */

    /* It's one of our principal hierarchies */

    return false;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getPrincipal(java.lang.String)
   */
  @Override
  public BwPrincipal getPrincipal(final String href) throws CalFacadeException {
    try {
      String uri = new URI(href).getPath();

      if (!isPrincipal(uri)) {
        return null;
      }

      int start = -1;

      int end = uri.length();
      if (uri.endsWith("/")) {
        end--;
      }

      for (String prefix: toWho.keySet()) {
        if (!uri.startsWith(prefix)) {
          continue;
        }

        int whoType = toWho.get(prefix);
        String who = null;
        start = prefix.length();

        if (start == end) {
          // Trying to browse user principals?
        } else if (uri.charAt(start) != '/') {
          throw new CalFacadeException(CalFacadeException.principalNotFound);
        } else if ((whoType == WhoDefs.whoTypeUser) ||
                   (whoType == WhoDefs.whoTypeGroup)) {
          /* Strip off the principal prefix for real users.
           */
          who = uri.substring(start + 1, end);
        } else {
          who = uri;
        }

        if (who == null) {
          return null;
        }

        BwPrincipal p = null;

        if (whoType == WhoDefs.whoTypeUser) {
          p = new BwUser(who);
          p.setPrincipalRef(uri);
        } else if (whoType == WhoDefs.whoTypeGroup) {
          p = new BwGroup(who);
          p.setPrincipalRef(uri);
        }

        return p;
      }

      throw new CalFacadeException(CalFacadeException.principalNotFound);
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#makePrincipalUri(java.lang.String, boolean)
   */
  @Override
  public String makePrincipalUri(final String id,
                                 final int whoType) throws CalFacadeException {
    if (isPrincipal(id)) {
      return id;
    }

    String root = fromWho.get(whoType);

    if (root == null) {
      throw new CalFacadeException(CalFacadeException.unknownPrincipalType);
    }

    return root + "/" + id;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getPrincipalRoot()
   */
  @Override
  public String getPrincipalRoot() throws CalFacadeException {
    return getSystemRoots().getPrincipalRoot();
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#getGroups(java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String>getGroups(String rootUrl,
                                     final String principalUrl) throws CalFacadeException {
    Collection<String> urls = new TreeSet<String>();

    if (principalUrl == null) {
      /* for the moment if the root url is the user principal hierarchy root
       * just return the current user principal
       */
      if (rootUrl.endsWith("/")) {
        rootUrl = rootUrl.substring(0, rootUrl.length() - 1);
      }

      /* ResourceUri should be the principals root or user principal root */
      if (!rootUrl.equals(getSystemRoots().getPrincipalRoot()) &&
          !rootUrl.equals(getSystemRoots().getUserPrincipalRoot())) {
        return urls;
      }

      urls.add(getSystemRoots().getUserPrincipalRoot() + "/" +
               cb.getCurrentUser().getAccount());
    } else {
      // XXX incomplete
    }

    return urls;
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#uriToCaladdr(java.lang.String)
   */
  @Override
  public String uriToCaladdr(final String val) throws CalFacadeException {
    /* Override this to do directory lookups or query vcard. The following
     * transforms may be insufficient
     */

    if (isPrincipal(val)) {
      // Leave as is
      return userToCaladdr(val);
    }

    boolean isAccount = true;

    /* check for something that looks like mailto:somebody@somewhere.com,
       scheduleto:, etc.  If exists, is not an internal Bedework account. */
    int colonPos = val.indexOf(":");
    int atPos = val.indexOf("@");
    String uri = val;

    if (colonPos > 0) {
      if (atPos < colonPos) {
        return null;
      }

      isAccount = false;
    } else if (atPos > 0) {
      uri = "mailto:" + val;
    }

    AccessPrincipal possibleAccount = caladdrToPrincipal(uri);
    if ((possibleAccount != null) &&       // Possible bedework user
        (!validPrincipal(possibleAccount.getPrincipalRef()))) {   // but not valid
      return null;
    }

    if (isAccount) {
      uri = userToCaladdr(uri);
    }

    return uri;
  }

  protected static class FlushingMap<K, V> extends HashMap<K, V> {
    @Override
    public synchronized V put(final K key, final V val) {
      if ((size() == maxCaMapSize) ||
          ((System.currentTimeMillis() - lastCaClear) > maxCaRefreshTime)) {
        clear();
      }

      return super.put(key, val);
    }

    @Override
    public V get(final Object key) {
      V val = super.get(key);

      if (val == null) {
        return null;
      }

      if ((size() == maxCaMapSize) ||
          ((System.currentTimeMillis() - lastCaClear) > maxCaRefreshTime)) {
        clear();
        super.put((K)key, val);
      }

      return val;
    }
  };

  private static final int maxCaMapSize = 3000;

  private static final long lastCaClear = System.currentTimeMillis();

  private static final long maxCaRefreshTime = 1000 * 60 * 5; // 5 minutes

  protected static Map<String, String> userToCalAddrMap = new FlushingMap<String, String>();

  protected static Map<String, BwPrincipal> calAddrToPrincipalMap = new FlushingMap<String, BwPrincipal>();

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#principalToCaladdr(org.bedework.calfacade.BwPrincipal)
   */
  @Override
  public String principalToCaladdr(final BwPrincipal val) throws CalFacadeException {
    return userToCaladdr(val.getAccount());
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#userToCaladdr(java.lang.String)
   */
  @Override
  public String userToCaladdr(final String val) throws CalFacadeException {
    /* Override this to do directory lookups or query vcard. The following
     * transforms may be insufficient
     */

    String ca = userToCalAddrMap.get(val);

    if (ca != null) {
      return ca;
    }

    if (isPrincipal(val)) {
      BwPrincipal p = getPrincipal(val);

      if (p.getKind() == WhoDefs.whoTypeUser) {
        ca = userToCaladdr(p.getAccount());
      } else {
        // Can't do anything with groups etc.
        ca = val;
      }

      userToCalAddrMap.put(val, ca);

      return ca;
    }

    getProps(); // Ensure all set up

    try {
      int atPos = val.indexOf("@");

      boolean hasMailto = val.toLowerCase().startsWith("mailto:");

      if (atPos > 0) {
        if (hasMailto) {
          // ensure lower case (helps with some tests)
          return "mailto:" + val.substring(7);
        }

        ca = "mailto:" + val;
        userToCalAddrMap.put(val, ca);

        return ca;
      }

      if (defaultDomain == null) {
        throw new CalFacadeException(CalFacadeException.noDefaultDomain);
      }

      StringBuilder sb = new StringBuilder();
      if (!hasMailto) {
        sb.append("mailto:");
      }

      sb.append(val);
      sb.append("@");
      sb.append(defaultDomain);

      ca = sb.toString();
      userToCalAddrMap.put(val, ca);

      return ca;
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calfacade.ifs.Directories#caladdrToPrincipal(java.lang.String)
   */
  @Override
  public BwPrincipal caladdrToPrincipal(final String caladdr) throws CalFacadeException {
    try {
      if (caladdr == null) {
        throw new CalFacadeException(CalFacadeException.nullCalendarUserAddr);
      }

      BwPrincipal p = calAddrToPrincipalMap.get(caladdr);
      if (p != null) {
        return p;
      }

      getProps(); // Ensure all set up

      if (isPrincipal(caladdr)) {
        p = getPrincipal(caladdr);
        calAddrToPrincipalMap.put(caladdr, p);

        return p;
      }

      String acc = null;
      String ca = caladdr;
      int atPos = ca.indexOf("@");

      if (atPos > 0) {
        ca = ca.toLowerCase();
      }

      if (onlyDomain != null) {
        if (atPos < 0) {
          acc = ca;
        }

        if (onlyDomain.matches(ca, atPos)) {
          acc = ca.substring(0, atPos);
        }
      } else if (atPos < 0) {
        // Assume default domain?
        acc = ca;
      } else if (anyDomain) {
        acc = ca;
      } else {
        for (DomainMatcher dm: domains) {
          if (dm.matches(ca, atPos)) {
            acc = ca;
            break;
          }
        }
      }

      if (acc == null) {
        // Not ours
        return null;
      }

      if (acc.toLowerCase().startsWith("mailto:")) {
        acc = acc.substring("mailto:".length());
      }

      //XXX -at this point we should validate the account

      int whoType = WhoDefs.whoTypeUser;

      for (CAPrefixInfo c: getCaPrefixInfo()) {
        if (acc.startsWith(c.getPrefix())) {
          whoType = c.getType();
          break;
        }
      }

      p = getPrincipal(makePrincipalUri(acc, whoType));
      calAddrToPrincipalMap.put(caladdr, p);

      return p;
    } catch (CalFacadeException cfe) {
      throw cfe;
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.CalSvcI#fixCalAddr(java.lang.String)
   */
  @Override
  public String fixCalAddr(final String val) throws CalFacadeException {
    if (val == null) {
      throw new CalFacadeException(CalFacadeException.badCalendarUserAddr);
    }

    if (val.startsWith("/")) {
      return val;
    }

    int colonPos = val.indexOf(":");
    int atPos = val.indexOf("@");

    if (colonPos > 0) {
      if (atPos < colonPos) {
        throw new CalFacadeException(CalFacadeException.badCalendarUserAddr);
      }

      return val;
    } else if (atPos > 0) {
      return "mailto:" + val;
    } else {
      // No colon - no at - maybe just userid
      if (defaultDomain == null) {
        throw new CalFacadeException(CalFacadeException.badCalendarUserAddr);
      }
      return "mailto:" + val + "@" + defaultDomain;
    }
  }

  /* ====================================================================
   *  Protected methods.
   * ==================================================================== */

  /** Return the name of the configuration properties for the module,
   * e.g "module.user-ldap-group" or "module.dir-config"
   * @return String
   */
  protected abstract String getConfigName();

  /** See if the given principal href is in our table. Allows us to short circuit
   * the validation process.
   *
   * @param href
   * @return true if we know about this one.
   */
  protected synchronized boolean lookupValidPrincipal(final String href) {
    return validPrincipals.containsKey(href);
  }

  /** Add a principal we have validated.
   *
   * @param href
   */
  protected void addValidPrincipal(final String href) {
    validPrincipals.put(href, href);
  }

  protected DirConfigProperties getProps() throws CalFacadeException {
    if (props != null) {
      return props;
    }

    try {
      props = (DirConfigProperties)CalOptionsFactory.getOptions().getGlobalProperty(getConfigName());

      String prDomains = props.getDomains();

      /* Convert domains list to a Collection */
      if (prDomains.equals("*")) {
        anyDomain = true;
      } else if ((prDomains.indexOf(",") < 0) &&
                 (!prDomains.startsWith("*"))) {
        onlyDomain = new DomainMatcher(prDomains);
      } else {
        domains = new ArrayList<DomainMatcher>();

        for (String domain: props.getDomains().split(",")) {
          domains.add(new DomainMatcher(domain));
        }
      }

      defaultDomain = props.getDefaultDomain();
      if ((defaultDomain == null) && (onlyDomain != null)) {
        defaultDomain = prDomains;
      }
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }

    return props;
  }

  protected SystemRoots getSystemRoots() throws CalFacadeException {
    if (sysRoots != null) {
      return sysRoots;
    }

    try {
      sysRoots = (SystemRoots)CalOptionsFactory.getOptions().getGlobalProperty("systemRoots");
    } catch (Throwable t) {
      throw new CalFacadeException(t);
    }

    return sysRoots;
  }

  protected CalAddrPrefixes getCaPrefixes() throws CalFacadeException {
    if (caPrefixes != null) {
      return caPrefixes;
    }

    try {
      caPrefixes = (CalAddrPrefixes)CalOptionsFactory.getOptions().getGlobalProperty("caladdrPrefixes");
    } catch (Throwable t) {
      return null;
    }

    return caPrefixes;
  }

  protected Collection<CAPrefixInfo> getCaPrefixInfo() throws CalFacadeException {
    if (caPrefixInfo != null) {
      return caPrefixInfo;
    }

    CalAddrPrefixes cap = getCaPrefixes();

    List<CAPrefixInfo> capInfo = new ArrayList<CAPrefixInfo>();

    if (cap != null) {
      addCaPrefix(capInfo, cap.getUser(), WhoDefs.whoTypeUser);
      addCaPrefix(capInfo, cap.getGroup(), WhoDefs.whoTypeGroup);
      addCaPrefix(capInfo, cap.getHost(), WhoDefs.whoTypeHost);
      addCaPrefix(capInfo, cap.getTicket(), WhoDefs.whoTypeTicket);
      addCaPrefix(capInfo, cap.getResource(), WhoDefs.whoTypeResource);
      addCaPrefix(capInfo, cap.getLocation(), WhoDefs.whoTypeVenue);
    }

    caPrefixInfo = Collections.unmodifiableList(capInfo);

    return caPrefixInfo;
  }

  private void addCaPrefix(final List<CAPrefixInfo> capInfo,
                           final String prefix, final int type) {
    if (prefix == null) {
      return;
    }

    capInfo.add(new CAPrefixInfo(prefix, type));
  }


  /** */
  public static class CardDavInfo {
    /** */
    public String host;
    /** */
    public int port;
    /** */
    public String contextPath;
  }

  protected CardDavInfo getCardDavInfo(final boolean auth) throws CalFacadeException {
    CardDavInfo cdi = new CardDavInfo();

    try {
      OptionsI opts = CalOptionsFactory.getOptions();
      if (auth) {
        cdi.host = (String)opts.getGlobalProperty("personalCardDAVHost");
        cdi.port = Integer.valueOf((String)opts.getGlobalProperty("personalCardDAVPort"));
        cdi.contextPath = (String)opts.getGlobalProperty("personalCardDAVContext");
      } else {
        cdi.host = (String)opts.getGlobalProperty("publicCardDAVHost");
        cdi.port = Integer.valueOf((String)opts.getGlobalProperty("publicCardDAVPort"));
        cdi.contextPath = (String)opts.getGlobalProperty("publicCardDAVContext");
      }
    } catch (Throwable t) {
      error(t);
      return null;
    }

    return cdi;
  }

  /* Get a logger for messages
   */
  protected Logger getLogger() {
    if (log == null) {
      log = Logger.getLogger(this.getClass());
    }

    return log;
  }

  protected void error(final Throwable t) {
    getLogger().error(this, t);
  }

  protected void warn(final String msg) {
    getLogger().warn(msg);
  }

  protected void trace(final String msg) {
    getLogger().debug(msg);
  }

  /* ====================================================================
   *  Private methods.
   * ==================================================================== */

  private void initWhoMaps(final String prefix, final int whoType) {
    toWho.put(prefix, whoType);
    fromWho.put(whoType, prefix);
  }
}

