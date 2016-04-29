/*-------------------------------------------------------------------------
 *
 * Merger.oz
 *
 *    Partition Merger for Beernet
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
 *    $Date: 2015-10-26 14:35:38 +0200 (Mon, 26 Oct 2015) $
 *
 * NOTES
 *      
 *    Periodically checks queue, and triggers lookup in case queue is nonempty
 *    When receives lookup request try to repair if appropriate.
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
   PbeerQueue  at '../../utils/PbeerQueue.ozf'
   Constants   at '../../commons/Constants.ozf'

export
   New

define

   Gamma       = Constants.mergerGamma    % Granularity to trigger merger
   M           = Constants.mergerMLookupsPerPeriod    % Number of MLookups in each period
   F           = Constants.mergerFanout

   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      SelfPbeer   % Pbeer reference assinged by a external component
      SelfQueue   % Queue assigned by an external components

      TheTimer    % Component that triggers timeout
      

      %% Launch the timer for every period
      proc {NewPeriod start}
         {TheTimer startTimer(Gamma)}
      end

      proc {Timeout timeout}
         proc {TriggerLookup I}
            if {Not {PbeerQueue.isEmpty @SelfQueue}} andthen
                (I=<M orelse M==100) then
                CurrentElement = {NewCell nil}
                in
                SelfQueue := {PbeerQueue.dequeue @SelfQueue CurrentElement}
                case @CurrentElement of element(Q F) then
                     {@Listener mlookup(src:@SelfPbeer id:Q fanout:F)} 
		     {ComLayer sendTo(Q mlookup(src:@SelfPbeer
                                              id:@SelfPbeer fanout:F) log:rlxring)}
                else
                     skip
                end
                {TriggerLookup I+1}
            end  
         end
         in

         if {Not {PbeerQueue.isEmpty @SelfQueue}} then
           {TriggerLookup 1}
         end
         {NewPeriod start}
      end 

      proc {MakeAQueueInsert makeAQueueInsert(P)}
          if P.id\=@SelfPbeer.id andthen {Not {PbeerQueue.isInQueue P @SelfQueue}} then
             SelfQueue := {PbeerQueue.enqueue element(P F) @SelfQueue}
          end
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
                  makeAQueueInsert: MakeAQueueInsert
                  start:         NewPeriod
                  timeout:       Timeout
                  )
   in
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}
      SelfQueue   = {NewCell {PbeerQueue.new}}

      Self        = {Component.new Events}
      Listener    = Self.listener
   
      {TheTimer setListener(Self.trigger)}
      
      {NewPeriod start}
      Self.trigger 
   end
end

