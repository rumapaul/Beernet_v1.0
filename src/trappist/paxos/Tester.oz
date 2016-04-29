%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor
import
   Application
   OS
   Property
   System
   Network        at '../../network/Network.ozf'
   PbeerMaker     at '../../pbeer/Pbeer.ozf'
define
   SIZE  = 42

   ComLayer
   MasterOfPuppets
   MasterId
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

   proc {Get Key}
      Value
   in
      {MasterOfPuppets get(Key Value)}
      {Wait Value}
      {System.show 'Getting'#Key#'we obtained'#Value}
   end

   proc {QuickRead Key}
      Value
   in
      {MasterOfPuppets getOne(Key Value trapp)}
      {Wait Value}
      {System.show 'Quick Reading'#Key#'we obtained'#Value}
   end

   proc {ReadAll Key}
      Val
   in
      {MasterOfPuppets getAll(Key Val trapp)}
      if {IsList Val} then skip end
      {System.show 'Reading All'#Key#'we obtained'#Val}
   end

   proc {ReadMajority Key}
      Val
   in
      {MasterOfPuppets getMajority(Key Val trapp)}
      if {IsList Val} then skip end
      {System.show 'Reading Majority'#Key#'we obtained'#Val}
   end

   proc {Trans1 TM}
      V
   in
      {TM write(foo bar)}
      {TM write(bink beer(name:adelardus style:dubbel alc:7))}
      {TM write(peter coenen)}
      V = {TM read(peter $)}
      {Wait V}
      {System.show 'the value'#V}
      {Wait Trigger}
      {TM commit}
   end

   proc {Trans2 TM}
      V
   in
      {TM write(foo flets)}
      {TM write(bink beer(name:adelardus style:trippel alc:8))}
      {TM write(peter pan)}
      V = {TM read(peter $)}
      {Wait V}
      {System.show 'the value'#V}
      {Wait Trigger2}
      {TM commit}
   end

   proc {Bind Var Id Low}
      thread
         Time
      in
         Time = Low + {OS.rand} mod 500
         {System.show Id#will_wait#Time}
         {Delay Time}
         Var = unit 
      end
   end

   Client Stream Trigger Trigger2

in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   {CreateNetwork}
   {System.show 'network created. Going to put, get and delete values'}
   {System.show 'waiting a bit'}
   {Delay 1000}
   Client = {NewPort Stream}

   {System.show 'waiting a bit more'}
   {MasterOfPuppets runTransaction(Trans1 Client paxos)}
   {Bind Trigger 1 0}
   {Wait Stream}
   Trigger2 = unit
   {Pbeers.2.2.2.1 runTransaction(Trans2 Client paxos)}
   {Wait Stream.2}
   {System.show 'the stream'#Stream}
   {QuickRead foo}
   {QuickRead bink}
   {QuickRead peter}
   {ReadAll foo}
   {ReadMajority foo}
   {ReadAll bink}
   {ReadMajority bink}
   {ReadAll peter}
   {ReadMajority peter}
   {QuickRead foo}
   {Get foo}
   {Get bink}
   {Get peter}
   {Application.exit 0}
end
