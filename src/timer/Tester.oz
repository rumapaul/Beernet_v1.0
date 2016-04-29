%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   System
   Component   at '../corecomp/Component.ozf'
   Timer       at 'Timer.ozf'

define
   
   fun {MakeTimerTester}

      Ref
      MyTimer
      Channel

      proc {MakeChannel Event}
         makeChannel = Event
      in
         {MyTimer setListener(Ref)}
         Channel := true
      end

      proc {TriggerEvent Event}
         triggerEvent(Time) = Event
      in
         if @Channel then
            {MyTimer startTrigger(Time zZz)}
         else
            {MyTimer startTrigger(Time zZz Ref)}
         end
      end
 
      proc {TriggerTimer Event}
         triggerTimer(Time) = Event
      in
         if @Channel then
            {MyTimer startTimer(Time)}
         else
            {MyTimer startTimer(Time Ref)}
         end
      end
 
      proc {Timeout Event}
         {System.show 'i got a timeout'}
      end

      proc {ZZZ Event}
         {System.show 'zZzZzZzZzZzZ'}
      end

      Events = events(
                  makeChannel:   MakeChannel
                  triggerEvent:  TriggerEvent
                  triggerTimer:  TriggerTimer
                  timeout:       Timeout
                  zZz:           ZZZ
                  )
   in
      Channel  = {NewCell false}
      Ref      = {Component.newTrigger Events}
      MyTimer  = {Timer.new}
      Ref
   end

   Tester

in

   Tester = {MakeTimerTester}
   {System.show foo}
   {Tester triggerEvent(8000)}
   {Tester triggerTimer(1000)}
   {Tester triggerTimer(4000)}
   {Tester triggerTimer(2000)}
   {Tester triggerEvent(5000)}
   {Tester triggerTimer(1000)}
   {Delay 8500}
   {System.showInfo "---going to test channel ---"}
   {Tester makeChannel}
   {Tester triggerEvent(3000)}
   {Tester triggerTimer(1000)}
end
