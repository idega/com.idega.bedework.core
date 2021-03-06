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

import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.BwView;
import org.bedework.calfacade.svc.prefs.BwPreferences;
import org.bedework.calsvci.ViewsI;

import java.util.Collection;
import java.util.TreeSet;

/** This acts as an interface to the database for views.
 *
 * @author Mike Douglass       douglm - rpi.edu
 */
class Views extends CalSvcDb implements ViewsI {
  Views(final CalSvc svci,
        final BwUser user) {
    super(svci, user);
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#add(org.bedework.calfacade.svc.BwView, boolean)
   */
  public boolean add(final BwView val,
                     final boolean makeDefault) throws CalFacadeException {
    if (val == null) {
      return false;
    }

    BwPreferences prefs = getSvc().getPrefsHandler().get();
    checkOwnerOrSuper(prefs);

    if (!prefs.addView(val)) {
      return false;
    }

    if (makeDefault) {
      prefs.setPreferredView(val.getName());
    }

    getSvc().getPrefsHandler().update(prefs);

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#remove(org.bedework.calfacade.svc.BwView)
   */
  public boolean remove(final BwView val) throws CalFacadeException{
    if (val == null) {
      return false;
    }

    BwPreferences prefs = getSvc().getPrefsHandler().get();
    checkOwnerOrSuper(prefs);

    //setupOwnedEntity(val, getUser());

    Collection<BwView> views = prefs.getViews();
    if ((views == null) || (!views.contains(val))) {
      return false;
    }

    String name = val.getName();

    views.remove(val);

    if (name.equals(prefs.getPreferredView())) {
      prefs.setPreferredView(null);
    }

    getSvc().getPrefsHandler().update(prefs);

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#find(java.lang.String)
   */
  public BwView find(String val) throws CalFacadeException {
    if (val == null) {
      BwPreferences prefs = getSvc().getPrefsHandler().get();

      val = prefs.getPreferredView();
      if (val == null) {
        return null;
      }
    }

    Collection<BwView> views = getAll();
    for (BwView view: views) {
      if (view.getName().equals(val)) {
        return view;
      }
    }

    return null;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#addCollection(java.lang.String, java.lang.String)
   */
  public boolean addCollection(final String name,
                               final String path) throws CalFacadeException {
    BwPreferences prefs = getSvc().getPrefsHandler().get();
    checkOwnerOrSuper(prefs);

    BwView view = find(name);

    if (view == null) {
      return false;
    }

    view.addCollectionPath(path);

    getSvc().getPrefsHandler().update(prefs);

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#removeCollection(java.lang.String, java.lang.String)
   */
  public boolean removeCollection(final String name,
                                  final String path) throws CalFacadeException {
    BwPreferences prefs = getSvc().getPrefsHandler().get(getPrincipal());
    checkOwnerOrSuper(prefs);

    BwView view = find(name);

    if (view == null) {
      return false;
    }

    view.removeCollectionPath(path);

    getSvc().getPrefsHandler().update(prefs);

    return true;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#getAll()
   */
  public Collection<BwView> getAll() throws CalFacadeException {
    Collection<BwView> c = getSvc().getPrefsHandler().get().getViews();
    if (c == null) {
      c = new TreeSet<BwView>();
    }
    return c;
  }

  /* (non-Javadoc)
   * @see org.bedework.calsvci.ViewsI#getAll(org.bedework.calfacade.BwUser)
   */
  public Collection<BwView> getAll(final BwUser user) throws CalFacadeException {
    Collection<BwView> c = getSvc().getPrefsHandler().get(user).getViews();
    if (c == null) {
      c = new TreeSet<BwView>();
    }
    return c;
  }
}
