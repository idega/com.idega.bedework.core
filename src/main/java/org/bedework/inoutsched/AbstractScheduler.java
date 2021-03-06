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
package org.bedework.inoutsched;

import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calsvc.CalSvcDb;
import org.bedework.calsvci.CalSvcFactoryDefault;
import org.bedework.calsvci.CalSvcI;
import org.bedework.calsvci.CalSvcIPars;

/** Handles a queue of scheduling requests. We need to delay
 * processing until after the initiating request is processed. In addition,
 * processing of the message can cause a significant amount of traffic as each
 * message can itself generate more messages.
 *
 * @author Mike Douglass
 */
public abstract class AbstractScheduler extends CalSvcDb implements ScheduleMesssageHandler {
  /**
   */
  public AbstractScheduler() {
    super(null, null);
  }

  /** Get an svci object as a different user.
   *
   * @param principalHref
   * @return CalSvcI
   * @throws CalFacadeException
   */
  protected CalSvcI getSvci(final String principalHref) throws CalFacadeException {
    CalSvcI svci;

    /* account is what we authenticated with.
     * user, if non-null, is the user calendar we want to access.
     */
    CalSvcIPars runAsPars = CalSvcIPars.getServicePars(principalHref,//principal.getAccount(),
                                                       false,   // publicAdmin
                                                       "/principals/users/root".equals(principalHref));  // allow SuperUser

    svci = new CalSvcFactoryDefault().getSvc(runAsPars);
    setSvc(svci);

    svci.open();
    svci.beginTransaction();

    return svci;
  }

  protected void rollback(final CalSvcI svci) {
    try {
      svci.rollbackTransaction();
    } catch (Throwable t) {
      // Pretty much screwed  now
    }
  }

  protected void closeSvci(final CalSvcI svci) throws CalFacadeException {
    if ((svci == null) || !svci.isOpen()) {
      return;
    }

    CalFacadeException exc = null;

    try {
      try {
        svci.endTransaction();
      } catch (CalFacadeException cfe) {
        rollback(svci);
        exc = cfe;
      }
    } finally {
      svci.close();
    }

    if (exc != null) {
      throw exc;
    }
  }
}
