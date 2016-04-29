/*-------------------------------------------------------------------------
 *
 * PeriodicStabilizer.oz
 *
 *    Periodic Stabilizer for Beernet
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 10 $ $Author: ruma $
 *
 *    $Date: 2015-10-29 12:31:14 +0200 (Thurs, 29 Oct 2015) $
 *
 * NOTES
 *      
 *    Periodically asks successor about it's predecessor, and triggers update in case
 *    any issue is found in neighbourhood.
 *
 * EVENTS
 *
 *    Accepts: eventName(arg1 argN) - Events that can be triggered on this
 *    component to request a service.
 *
 *    Confirmation: eventName(arg1 argN) - Events used by a component to
 *    confirm the completion of a request.
 *
 *    Indication: eventName(arg1 argN) - Events used by a given component to
 *    deliver information to another component
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../../corecomp/Component.ozf'
   Timer       at '../../timer/Timer.ozf'
   Constants   at '../../commons/Constants.ozf'
export
   New

define

   DELTA       = Constants.pSPeriod    % Granularity to trigger stabilization

   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      SelfPbeer   % Pbeer reference assinged by an external component

      TheTimer    % Component that triggers timeout

      %% Launch the timer for every period
      proc {NewPeriod start}
         {TheTimer startTimer(DELTA)}
      end

      proc {Timeout timeout}
         {@Listener stabilize}
         %{Delay 1000} 
         {NewPeriod start}
      end

      proc {SetPbeer setPbeer(NewPbeer)}
         SelfPbeer := NewPbeer
      end

      proc {SetComLayer setComLayer(TheComLayer)}
         ComLayer = TheComLayer
         SelfPbeer := {ComLayer getRef($)} 
      end

      Events = events(
                  setPbeer:      SetPbeer
                  setComLayer:   SetComLayer
                  start:         NewPeriod
                  timeout:       Timeout
                  )
   in
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      Self        = {Component.new Events}
      Listener    = Self.listener
   
      {TheTimer setListener(Self.trigger)}
      
      {NewPeriod start}
      Self.trigger 
   end
end

