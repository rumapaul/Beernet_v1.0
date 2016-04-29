/*-------------------------------------------------------------------------
 *
 * Timer.oz
 *
 *    Provies a timer that triggers timeout on the caller component
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: boriss $
 *
 *    $Date: 2011-05-19 21:45:21 +0200 (Thu, 19 May 2011) $
 *
 * NOTES
 *      
 *    This component has no state. It receives only one event on request, which
 *    is 'startTimer', with a time and a component as parameter. Once the time
 *    has passed, it triggers (as indication) the event 'timeout' on the caller
 *    component.
 *
 * EVENTS
 *
 *    Accepts: startTimer(Time Component) - Start the timer that will confirm
 *    to Component after Time milliseconds.
 *
 *    Confirmation: timeout - Used to indicate that Time milliseconds has
 *    passed.
 *
 *    Accepts: startTrigger(Time Component Event) - Start the timer that will
 *    confirm not with a timeout, but with Event
 *
 *    Confirmation: whatever Event is asked to be triggered after Time
 *    milliseconds has passed.
 *
 *-------------------------------------------------------------------------
 */

functor

import
   Component   at '../corecomp/Component.ozf'

export
   New

define   

   fun {New}

      Self

      %% --- Utils ---
      proc {Timer Time Event Component}
         thread
            {Delay Time}
            {Component Event}
         end
      end
         
      %%--- Events ---
      proc {StartTimer Event}
         case Event
         of startTimer(Time Component) then
            {Timer Time timeout Component}
         [] startTimer(Time) then
            {Timer Time timeout @(Self.listener)}
         end
      end
   
      proc {StartTrigger Event}
         case Event
         of startTrigger(Time TheEvent Component) then
            {Timer Time TheEvent Component}
         [] startTrigger(Time TheEvent) then
            {Timer Time TheEvent @(Self.listener)}
         end
      end

      Events = events(
                  startTimer:    StartTimer
                  startTrigger:  StartTrigger
                  )
   in
      Self = {Component.new Events}
      Self.trigger
   end

end
