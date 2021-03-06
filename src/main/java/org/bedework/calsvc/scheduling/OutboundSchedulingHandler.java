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
package org.bedework.calsvc.scheduling;

import org.bedework.calfacade.BwAttendee;
import org.bedework.calfacade.BwCalendar;
import org.bedework.calfacade.BwEvent;
import org.bedework.calfacade.BwPrincipal;
import org.bedework.calfacade.BwUser;
import org.bedework.calfacade.BwXproperty;
import org.bedework.calfacade.RecurringRetrievalMode;
import org.bedework.calfacade.RecurringRetrievalMode.Rmode;
import org.bedework.calfacade.ScheduleResult;
import org.bedework.calfacade.ScheduleResult.ScheduleRecipientResult;
import org.bedework.calfacade.exc.CalFacadeAccessException;
import org.bedework.calfacade.exc.CalFacadeException;
import org.bedework.calfacade.svc.EventInfo;
import org.bedework.calsvc.CalSvc;
import org.bedework.icalendar.Icalendar;

import edu.rpi.cmt.access.PrivilegeDefs;
import edu.rpi.cmt.calendar.IcalDefs;
import edu.rpi.cmt.calendar.ScheduleStates;
import edu.rpi.sss.util.Uid;
import edu.rpi.sss.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** Rather than have a single class steering calls to a number of smaller classes
 * we will build up a full implementation by progressively implementing abstract
 * classes.
 *
 * <p>That allows us to split up some rather complex code into appropriate pieces.
 *
 * <p>This piece handles outbound schduling methods - those going to an inbox
 *
 * @author douglm
 *
 */
public abstract class OutboundSchedulingHandler extends IScheduleHandler {
  OutboundSchedulingHandler(final CalSvc svci,
                            final BwUser user) {
    super(svci, user);
  }

  /* Send the meeting request. If recipient is non-null send only to that recipient
   * (used for REFRESH handling), otherwise send to recipients in event.
   */
  protected void sendSchedule(final ScheduleResult sr,
                              final EventInfo ei,
                              final String recipient,
                              final String fromAttUri,
                              final boolean fromOrganizer) throws CalFacadeException {
    /* Recipients external to the system. */
    BwEvent ev = ei.getEvent();
    boolean freeBusyRequest = ev.getEntityType() ==
      IcalDefs.entityTypeFreeAndBusy;

    ev.updateDtstamp();

    if (recipient != null) {
      getRecipientInbox(ei, recipient, fromAttUri, sr, freeBusyRequest);
    } else {
      for (String recip: ev.getRecipients()) {
        getRecipientInbox(ei, recip, fromAttUri, sr, freeBusyRequest);
      }
    }

    if (sr.errorCode != null) {
      /* Cannot continue if any disallowed
       */
      // return externalRcs;
    }

    /* As we go through the inbox info, we gather together those for the same
     * host but external to this system.
     *
     * We then send off one request to each external host.
     */
    Map<String, Collection<UserInbox>> hostMap = new HashMap<String,
                                                             Collection<UserInbox>>();

    for (ScheduleRecipientResult sres: sr.recipientResults.values()) {
      UserInbox ui = (UserInbox)sres;

      if (sr.ignored) {
        ui.status = ScheduleStates.scheduleIgnored;
        continue;
      }

      if (ui.status == ScheduleStates.scheduleUnprocessed) {
        if (ui.host != null) {
          Collection<UserInbox> inboxes = hostMap.get(ui.host.getHostname());

          if (inboxes == null) {
            inboxes = new ArrayList<UserInbox>();
            hostMap.put(ui.host.getHostname(), inboxes);
          }

          inboxes.add(ui);

          continue;
        }

        String deliveryStatus = null;

        try {
          if (freeBusyRequest) {
            sres.freeBusy = getFreeBusy(null, ui.principal,
                                        ev.getDtstart(), ev.getDtend(),
                                        ev.getOrganizer(),
                                        ev.getUid(),
                                        null);

            ui.status = ScheduleStates.scheduleOk;
          } else if (!ui.principal.getPrincipalRef().equals(getPrincipal().getPrincipalRef())) {
            if (addToInbox(ui.inboxPath, ui.principal, ei,
                           fromOrganizer) == null) {
              ui.status = ScheduleStates.scheduleOk;
              deliveryStatus = IcalDefs.deliveryStatusDelivered;
            } else {
              ui.status = ScheduleStates.scheduleError;
              deliveryStatus = IcalDefs.deliveryStatusFailed;
            }
          } else {
            // That's us
            ui.setAttendeeScheduleStatus(null);
            ui.status = ScheduleStates.scheduleOk;
          }
        } catch (CalFacadeAccessException cae) {
          ui.status = ScheduleStates.scheduleNoAccess;
          deliveryStatus = IcalDefs.deliveryStatusNoAccess;
        }

        if (fromOrganizer) {
          if (deliveryStatus != null) {
            ui.setAttendeeScheduleStatus(deliveryStatus);
          }
        } else {
          ev.getOrganizer().setScheduleStatus(deliveryStatus);
        }
      }

      if (debug) {
        trace("added recipient " + ui.recipient + " status = " + ui.status);
      }
    }

    for (Collection<UserInbox> inboxes: hostMap.values()) {
      /* Send any realtime requests to external servers. */
      sendExternalRequest(sr, ei, inboxes);
    }
  }

  /** Add a copy of senderEi to the users inbox and add to the autoschedule queue.
   * The 'sender' may be the organizer of a meeting, if it's REQUEST etc, or the
   * attendee replying.
   *
   * @param inboxPath
   * @param attPrincipal
   * @param senderEi
   * @param fromOrganizer
   * @return null for ok, errorcode otherwise
   * @throws CalFacadeException
   */
  private String addToInbox(final String inboxPath,
                            final BwPrincipal attPrincipal,
                            final EventInfo senderEi,
                            final boolean fromOrganizer) throws CalFacadeException {
    EventInfo ei = copyEventInfo(senderEi, fromOrganizer, attPrincipal);
    BwEvent ev = ei.getEvent();

    if (senderEi.getReplyUpdate()) {
      // Flag as a trivial update to atendee status
      ev.addXproperty(new BwXproperty(BwXproperty.bedeworkSchedulingReplyUpdate,
                                      null, "true"));
    }

    // Recipients should not be able to see other recipients.

    ev.setRecipients(null);
    ev.addRecipient(getSvc().getDirectories().principalToCaladdr(attPrincipal));

    /*
    if (destAtt != null) {
      String attPartStat = destAtt.getPartstat();

      if ((attPartStat == null) ||   // default - needs-action
          (!attPartStat.equalsIgnoreCase(IcalDefs.partstatValAccepted) &&
           !attPartStat.equalsIgnoreCase(IcalDefs.partstatValCompleted) &&
           !attPartStat.equalsIgnoreCase(IcalDefs.partstatValDelegated))) {
        ev.setTransparency(IcalDefs.transparencyTransparent);
      }
    }*/

    String evDtstamp = ev.getDtstamp();

    ev.setScheduleState(BwEvent.scheduleStateNotProcessed);
    ev.setColPath(inboxPath);

    /* Before we add this we should see if there is an earlier one we can
     * discard. As attendees update their status we get many requests sent to
     * each attendee.
     *
     * Also this current message may be earlier than one already in the inbox.
     */

    int smethod = ev.getScheduleMethod();

    if (Icalendar.itipRequestMethodType(smethod)) {
      RecurringRetrievalMode rrm =
        new RecurringRetrievalMode(Rmode.overrides);

      Collection<EventInfo> inevs = getEvents(inboxPath,
                                              ev.getUid(),
                                              ev.getRecurrenceId(),
                                              true,
                                              rrm);

      for (EventInfo inei: inevs) {
        BwEvent inev = inei.getEvent();

        int cres = evDtstamp.compareTo(inev.getDtstamp());

        if (cres <= 0) {
          // Discard the new one
          return null;
        }

        /* Discard the earlier message */

        /* XXX What if this message is currently being processed by the inbox
         * handler process? Does it matter - will it reappear?
         *
         * Probably need to handle stale-state exceptions at the other end.
         */

        deleteEvent(inei, true, false);
      }
    }

    /* Add it and post to the autoscheduler */
    String ecode = addEvent(ei, "In-" + Uid.getUid() + "-" + evDtstamp,
                            BwCalendar.calTypeInbox,
                            true);

    if (ecode != null) {
      return ecode;
    }

    if (debug) {
      trace("Add event with name " + ev.getName() +
            " and summary " + ev.getSummary() +
            " to " + ev.getColPath());
    }

    addAutoScheduleMessage(true,
                           attPrincipal.getPrincipalRef(),
                           ev.getName(),
                           ev.getRecurrenceId());

    return null;
  }

  /* Get the inbox for the recipient. If the recipient is not local to this
   * system, we mark the inbox entry as deferred and add the recipient to the
   * list of external recipients. We will possibly mail the request or try
   * ischedule to another server.
   *
   * If fromAtt is not null and the recipient is that attendee we skip it. This
   * is the result of a reply from that attendee being broadcast to the other
   * attendees.
   *
   * If reinvite is true we are resending the invitation to all attendees,
   * including those who previously declined. Otherwise we skip those who
   * declined.
   *
   * Note we have to search all overrides to get the information we need. We can
   * short circuit this to some extent as we fill in information about attendees.
   */
  private void getRecipientInbox(final EventInfo ei,
                                 final String recip,
                                 final String fromAttUri,
                                 final ScheduleResult sr,
                                 final boolean freeBusyRequest)
  throws CalFacadeException {
    BwEvent ev = ei.getEvent();

    /* See if the attendee is in this event */
    BwAttendee att = ev.findAttendee(recip);

    if ((att != null) && (fromAttUri != null) &&
        fromAttUri.equals(att.getAttendeeUri())) {
      // Skip this one, they were the ones that sent the reply.
      return;
    }

    UserInbox ui = getInbox(sr, recip, freeBusyRequest);

    if (att != null) {
      ui.addAttendee(att);

      if (Util.compareStrings(att.getPartstat(),
                              IcalDefs.partstatValDeclined) == 0) {
        // Skip this one, they declined.
        return;
      }

      att.setScheduleStatus(IcalDefs.deliveryStatusPending);
    }

    if (ui.status == ScheduleStates.scheduleDeferred) {
      sr.externalRcs.add(recip);
    } else if (ui.status == ScheduleStates.scheduleNoAccess) {
      sr.errorCode = CalFacadeException.schedulingAttendeeAccessDisallowed;
      if (att != null) {
        att.setScheduleStatus(IcalDefs.deliveryStatusNoAccess);
      }
    } else if ((ui.principal == null) && (ui.host != null)) {
      sr.externalRcs.add(recip);
    }

    if (ei.getNumOverrides() > 0) {
      for (EventInfo oei: ei.getOverrides()) {
        getRecipientInbox(oei, recip, fromAttUri, sr, freeBusyRequest);
      }
    }
  }

  /* Return with deferred for external user.
   */
  private UserInbox getInbox(final ScheduleResult sr,
                             final String recipient,
                             final boolean freeBusyRequest) throws CalFacadeException {
    UserInbox ui = (UserInbox)sr.recipientResults.get(recipient);

    if (ui != null) {
      return ui;
    }

    ui = new UserInbox();
    sr.recipientResults.put(recipient, ui);

    BwPrincipal principal = getSvc().getDirectories().caladdrToPrincipal(recipient);
    ui.recipient = recipient;

    if (principal == null) {
      /* External to the system */
      ui.host = getSvc().getHostsHandler().getHostForRecipient(recipient);

      if (ui.host == null) {
        ui.status = ScheduleStates.scheduleDeferred;
        return ui;
      }

      if (freeBusyRequest) {
        // All can handle that
        return ui;
      }

      if (!ui.host.getSupportsISchedule() &&
          !ui.host.getSupportsCaldav() &&
          !ui.host.getSupportsBedework()) {
        ui.status = ScheduleStates.scheduleDeferred;
      }

      return ui;
    }

    try {
      if (principal.getPrincipalRef().equals(getPrincipal().getPrincipalRef())) {
        /* This is our own account. Let's not add it to our inbox.
         */
        ui.principal = getPrincipal();
        ui.status = ScheduleStates.scheduleUnprocessed;
        return ui;
      }

      ui.principal = principal;

      int priv;
      if (freeBusyRequest) {
        priv = PrivilegeDefs.privScheduleFreeBusy;
      } else {
        priv = PrivilegeDefs.privScheduleRequest;
      }

      BwCalendar inbox = getSpecialCalendar(ui.principal,
                                            BwCalendar.calTypeInbox,
                                            true, priv);
      if (inbox == null) {
        ui.status = ScheduleStates.scheduleNoAccess;
      } else {
        ui.inboxPath = inbox.getPath();
      }
    } catch (CalFacadeAccessException cae) {
      ui.status = ScheduleStates.scheduleNoAccess;
    }

    return ui;
  }
}
