/*-------------------------------------------------------------------------
 *
 * pp2p.oz
 *
 *    Implements perfect point-to-point link from Guerraoui's book
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *            Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 420 $ $Author: ruma $
 *
 *    $Date: 2016-04-15 15:43:52 +0200 (Fri, 15 April 2016) $
 *
 * NOTES
 *      
 *    This is an implementation of module 2.3 of R. Guerraouis book on reliable
 *    distributed programming. Properties "reliable delivery", "no duplication"
 *    and "no creation" are guaranteed by the implementation of Port in Mozart.
 *
 * EVENTS
 *
 *    Accepts: pp2pSend(Dest Msg) - Sends message Msg to destination Dest. Dest
 *    must be an Oz Port
 *
 *    Indication: pp2pDeliver(Src Msg) - Delivers message Msg sent by source
 *    Src.
 *    
 *-------------------------------------------------------------------------
 */

functor

import
   Component   at '../corecomp/Component.ozf'
   Timer       at '../timer/Timer.ozf'
   BootTime    at 'x-oz://boot/Time'
 
export
   New

define

   DELTA       = 20000    % Granularity to tune the link delay (in sync with failure detector)
   INIT_DELAY     = 0   % Initial Delay value
   MAX_DELAY = 30000   % Delay must not go beyond this value

   fun {New}
      SitePort       % Port to receive messages
      Listener       % Upper layer component
      FullComponent  % This component
      DelayPeriod      % Link Delay Knob 
      AllLinkDelays   %Delay Periods for All links
      MessageBuffer  % Buffer for messages to simulate network delays     
      DelayTimerFlag % Flag to determine to the clycle of delay simulations
      MinTimeoutPeriod %The granularity for the timer 
      TheTimer       % Component that triggers timeout

      proc {GetPort getPort(P)}
         P = SitePort
      end

      proc {PP2PSend pp2pSend(Dest Msg)}
         try
            thread
               if @DelayPeriod > 0 then
                  {Delay @DelayPeriod}
               end
               if {Value.hasFeature @AllLinkDelays Dest.id} then
                   MessageBuffer := message(dest:Dest msg:Msg 
                                           timestamp:{BootTime.getReferenceTime})|@MessageBuffer
                   if {Not @DelayTimerFlag} then
                       {TheTimer startTimer(@MinTimeoutPeriod)}
                       DelayTimerFlag := true
                   end
                   %{Delay @AllLinkDelays.(Dest.id)}
               else
                   {Port.send Dest.port SitePort#Msg}
               end
            end
            %{Port.send Dest SitePort#Msg}
         catch _ then
            %% TODO: improve exception handling
            skip
         end
      end

      proc {HandleMessages Str}
         case Str
         of (Src#Msg)|NewStr then
            {@Listener pp2pDeliver(Src Msg)}
            {HandleMessages NewStr}
         [] nil then % Port close
            skip
         %% To avoid crashing when the format is not respected,
         %% uncomment the else statement
         %else
         %   {HandleMessages Str.2}
         end
      end

      proc {InjectLinkDelay injectLinkDelay}
          if @DelayPeriod + DELTA =< MAX_DELAY then
              DelayPeriod := @DelayPeriod + DELTA
          end
      end

      proc {InjectLowLinkDelay injectLowLinkDelay}
          if @DelayPeriod > INIT_DELAY then
              DelayPeriod := @DelayPeriod - DELTA
          end
      end

      proc {InjectNoLinkDelay injectNoLinkDelay}
          DelayPeriod := INIT_DELAY
      end

       proc {InjectDelayVariance injectDelayVariance(Dest PeriodToVary DirectionFlag)}
          CurrentDelay NewDelay
          in
          if {Value.hasFeature @AllLinkDelays Dest} then
              CurrentDelay = @AllLinkDelays.Dest
          else 
              CurrentDelay = 0
          end 
          if DirectionFlag == 1 then
             NewDelay = CurrentDelay+PeriodToVary
          else
             NewDelay = CurrentDelay - PeriodToVary
          end

          if NewDelay > 0 then
                AllLinkDelays := {Record.adjoinAt @AllLinkDelays Dest NewDelay}
                if @MinTimeoutPeriod == 0 orelse NewDelay<@MinTimeoutPeriod then
                   MinTimeoutPeriod := NewDelay
                end
          else
                AllLinkDelays := {Record.subtract @AllLinkDelays Dest}
          end
      end

      proc {SimulateALinkDelay simulateALinkDelay(Dest Period)}
          if Period > 0 then
             AllLinkDelays := {Record.adjoinAt @AllLinkDelays Dest Period}
             if @MinTimeoutPeriod == 0 orelse Period<@MinTimeoutPeriod then
                 MinTimeoutPeriod := Period
             end
          else
             AllLinkDelays := {Record.subtract @AllLinkDelays Dest}
          end
      end

      proc {ARound timeout}
          CurrentRefTime = {BootTime.getReferenceTime}
          TmpMsgBuffer = {NewCell nil}
          in
          for AMessage in @MessageBuffer do
              if {Value.hasFeature @AllLinkDelays AMessage.dest.id} andthen
		 (CurrentRefTime-(AMessage.timestamp))>=@AllLinkDelays.(AMessage.dest.id) then
                   {Port.send AMessage.dest.port SitePort#(AMessage.msg)}
              else
                   TmpMsgBuffer := AMessage|@TmpMsgBuffer
              end
          end
          MessageBuffer:=@TmpMsgBuffer
          if @MessageBuffer\=nil then
             {TheTimer startTimer(@MinTimeoutPeriod)}
          else
	     DelayTimerFlag := false
          end 
      end

      Events = events(
                  getPort:            GetPort
                  pp2pSend:           PP2PSend
                  injectLinkDelay:    InjectLinkDelay
                  injectLowLinkDelay: InjectLowLinkDelay
                  injectNoLinkDelay:  InjectNoLinkDelay
                  injectDelayVariance: InjectDelayVariance
                  simulateALinkDelay:  SimulateALinkDelay
                  timeout:	       ARound   	
                  )

   in
      DelayPeriod = {NewCell INIT_DELAY}
      AllLinkDelays = {NewCell linkdelays}
      MessageBuffer = {NewCell nil}
      DelayTimerFlag = {NewCell false}
      MinTimeoutPeriod = {NewCell 0}

      TheTimer    = {Timer.new}
       
      local
         Stream
      in
         {Port.new Stream SitePort}
         thread
            {HandleMessages Stream}
         end
      end
      FullComponent = {Component.new Events}
      Listener = FullComponent.listener
      {TheTimer setListener(FullComponent.trigger)}
      FullComponent.trigger
   end
end
