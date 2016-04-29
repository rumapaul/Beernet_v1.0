%% This file is meant to test the functionality of the functors implemented on
%% this module using two different unix processes

functor

import
   Application
   Connection
   OS
   Pickle
   System
   Component   at '../corecomp/Component.ozf'
   Network     at 'Network.ozf'

define

   fun {MakeCoordinator}
      ComLayer
      Self

      TheStarter
      TheOther

      proc {Starter Event}
         starter(Server Receiver) = Event
      in
         TheStarter  = Server
         TheOther    = Receiver
      end

      proc {Simple Event}
         simple(Receiver Server) = Event
      in
         TheOther    = Receiver
         TheStarter  = Server
      end

      proc {ToTicket Event}
         toTicket(FileName) = Event
      in
         {Pickle.save   {Connection.offerUnlimited {ComLayer getRef($)}}
                        FileName}
      end

      proc {Finish Event}
         finish = Event
      in
         {Application.exit 0}
      end

      Events = events(
                  finish:     Finish
                  starter:    Starter
                  simple:     Simple
                  toTicket:   ToTicket
                  )
   in
      ComLayer = {Network.new}
      Self = {Component.newTrigger Events}
      {ComLayer setListener(Self)}
      Self
   end

   Coordinator
in
   {System.show 'Creating coordinator'}
   Coordinator = {MakeCoordinator}
   {System.show 'Coordinator created'}
   {Coordinator toTicket('coordinator.tket')}
   {System.show 'Ticket created'}
   {OS.system '#'("./testerDistNode --id foo"
                                  " --coordinator coordinator.tket"
                                  " --start true"
                                  " --total 11 &") _}
   {System.show 'Server launched'}
   {OS.system '#'("./testerDistNode --id bar"
                                  " --coordinator coordinator.tket &") _}
   {System.show 'Receiver launched'}
end
