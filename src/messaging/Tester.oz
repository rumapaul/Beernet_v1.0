%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   OS
   Property
   System
   Network        at '../network/Network.ozf'
   PbeerMaker     at '../pbeer/Pbeer.ozf'

define
   SIZE  = 42

   ComLayer
   MasterOfPuppets
   MasterId
   MaxKey
   Pbeers
   NetRef

   proc {CreateNetwork}
      %{System.show 'first line'}
      MasterOfPuppets = {PbeerMaker.new args}
      %{System.show 'second line'}
      MasterId = {MasterOfPuppets getId($)}
      Pbeers = {List.make SIZE-1}
      NetRef = {MasterOfPuppets getFullRef($)}
      for Pbeer in Pbeers do
         Pbeer = {PbeerMaker.new args}
         {Pbeer join(NetRef)}
         thread
            Id
            proc {ReceivingLoop}
               NewMsg
            in
               {Pbeer receive(NewMsg)}
               {Wait NewMsg}
               {System.show 'Pbeer '#Id#' got '#NewMsg.text#' from '#NewMsg.src}
               {ReceivingLoop}
            end
         in
            Id = {Pbeer getId($)}
            {ReceivingLoop}
         end
         %{Delay 100}
      end
      ComLayer = {Network.new}
      {Delay 1000}
      local
         P I S
      in
         {MasterOfPuppets getPred(P)}
         {MasterOfPuppets getId(I)}
         {MasterOfPuppets getSucc(S)}
         {System.showInfo "MASTER: "#P.id#"<-"#I#"->"#S.id}
      end
   end

   proc {SendAMsg Text}
      To
   in
      To = {OS.rand} mod MaxKey
      {System.show 'Sending '#Text#' to '#To}
      {MasterOfPuppets send(msg(text:Text src:MasterId) to:To)}
   end

in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   {CreateNetwork}
   MaxKey = {MasterOfPuppets getMaxKey($)}
   {System.show 'network created. Going to send messages'}
   for I in 1..10 do
      {Delay 333}
      {SendAMsg 'msg '#I}
   end
   {Delay 1000}
   local
      Flag
   in
      {MasterOfPuppets refreshFingers(Flag)}
      {System.show 'waiting for fingers refreshing'}
      {Wait Flag}
   end
   for I in 1..10 do
      {Delay 333}
      {SendAMsg 'msg '#I}
   end
   {Delay 1000}
   {Application.exit 0}
end
