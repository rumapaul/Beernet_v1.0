/*-------------------------------------------------------------------------
 *
 * PeriodicPhaseCalculator.oz
 *
 *    Periodic Phase Calculator for a node of Beernet
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 4 $ $Author: ruma $
 *
 *    $Date: 2016-02-05 19:03:00 +0200 (Fri, 5 February 2016) $
 *
 * NOTES
 *      
 *    Periodically compute phase of a node based on its succ, pred and fingertable, 
 *  		if phase is changed inform application layer
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

   DELTA       = Constants.phasePeriod    % Granularity to trigger stabilization

   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      Suicide
      SelfPbeer   % Pbeer reference assinged by an external component

      TheTimer    % Component that triggers timeout

      %% Launch the timer for every period
      proc {NewPeriod start}
         {TheTimer startTimer(DELTA)}
      end

      proc {Timeout timeout}
         {@Listener computePhase}
         {NewPeriod start}
      end

      proc {SetPbeer setPbeer(NewPbeer)}
         SelfPbeer := NewPbeer
      end

      proc {SetComLayer setComLayer(TheComLayer)}
         ComLayer = TheComLayer
         SelfPbeer := {ComLayer getRef($)} 
      end

      proc {StopPhaseTracker Event}
         {Suicide}
      end

      Events = events(
                  setPbeer:      SetPbeer
                  setComLayer:   SetComLayer
                  start:         NewPeriod
                  timeout:       Timeout
                  stopPhaseTracker: StopPhaseTracker
                  )
   in
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      Self        = {Component.new Events}
      Listener    = Self.listener
      Suicide     = Self.killer
   
      {TheTimer setListener(Self.trigger)}
      
      {NewPeriod start}
      Self.trigger 
   end
end

