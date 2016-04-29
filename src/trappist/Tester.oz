%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   Property
   System
   Network        at '../network/Network.ozf'
   PbeerMaker     at '../pbeer/Pbeer.ozf'
   Utils          at '../utils/Misc.ozf'

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

   proc {Put Key Value}
      {System.show 'Going to put'#Value#'with Key'#Key}
      {MasterOfPuppets put(Key Value)}
   end

   proc {Get Key}
      Value
   in
      {MasterOfPuppets get(Key Value)}
      {Wait Value}
      {System.show 'Getting'#Key#'we obtained'#Value}
   end

   proc {GetOne Key}
      Value
   in
      %% TODO: remove trapp and make getOne more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {MasterOfPuppets getOne(Key Value trapp)}
      {Wait Value}
      {System.show 'Getting one replica of '#Key#' we obtained '#Value}
   end

   proc {GetAll Key}
      Val
   in
      %% TODO: remove trapp and make getAll more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {MasterOfPuppets getAll(Key Val trapp)}
      if {IsList Val} then {System.showInfo "Inconsistencies"} end
      {System.show 'Reading All '#Key#' we obtained '#Val}
   end

   proc {GetMajority Key}
      Val
   in
      %% TODO: remove trapp and make getMajority more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {MasterOfPuppets getMajority(Key Val trapp)}
      if {IsList Val} then {System.showInfo "Inconsistencies"} end
      {System.show 'Reading Majority '#Key#' we obtained '#Val}
   end

   proc {Delete Key}
      {System.show 'Deleting'#Key}
      {MasterOfPuppets delete(Key)}
   end

   proc {Write Pairs Client Protocol Action}
      proc {Trans TM}
         for Key#Value in Pairs do
            {TM write(Key Value)}
         end
         {TM Action}
      end
   in
      {MasterOfPuppets runTransaction(Trans Client Protocol)}
   end

   fun {WaitStream Str N}
      fun {WaitLoop S I}
         {Wait S.1}
         if I == N then
            S.1
         else
            {WaitLoop S.2 I+1}
         end
      end
   in
      {WaitLoop Str 1}
   end

   proc {FullGetKeys L}
      for Key in L do
         {GetOne Key}
      end
      {System.showInfo "Majority:"}
      for Key in L do
         {GetMajority Key}
      end
      {System.showInfo "All"}
      for Key in L do
         {GetAll Key}
      end
   end

in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   {CreateNetwork}
   MaxKey = {MasterOfPuppets getMaxKey($)}
   /*{System.show 'network created. Going to put, get and delete values'}
   {Put foo bar}
   {Put beer net}
   {Put bink beer(name:adelardus style:dubbel alc:7)}
   {System.show 'waiting a bit'}
   {Delay 1000}
   {Get foo}
   {Get beer}
   {Get bink}
   {GetOne foo}
   {GetOne beer}
   {GetOne bink}
   {Put foo flets}
   {Get foo}
   {GetOne foo}
   {GetAll foo}
   {GetMajority foo}
   {GetAll foooo}
   {GetMajority foooo}
   {System.show '---- testing some message sending ----'}
   {MasterOfPuppets send(msg(text:'hello nurse' src:foo) to:{Utils.hash foo MaxKey})}
   {MasterOfPuppets send(msg(text:bla src:foo) to:{Utils.hash ina MaxKey})}
   {Delay 1000}
   {System.show 'TESTING DIRECT ACCESS TO THE STORE OF A PEER'}
   local
      Pbeer
   in
      {MasterOfPuppets lookup(key:foo res:Pbeer)}
      {MasterOfPuppets send(put(foo bla) to:Pbeer.id)}
   end
   {Delay 1000}
   {Get foo}
   {GetOne foo}
   local
      Pbeer HKey
   in
      HKey = {Utils.hash foo MaxKey} 
      {MasterOfPuppets lookupHash(hkey:HKey res:Pbeer)}
      %{MasterOfPuppets send(putItem(HKey foo tetete tag:dht) to:Pbeer.id)}
      {MasterOfPuppets send(put(foo tetete) to:Pbeer.id)}
   end
   {Delay 1000}
   {Get foo}
   {GetOne foo}
   {GetOne foo}
   {Delete nada}
   {Delay 1000}
   {Delete foo}
   {Delay 500}
   {Get foo}
   {GetOne foo}*/
   local
      P S RSetFlag
   in
      P = {NewPort S}
      {System.show '----- Trying out transactions -----'}
      {System.show '-----------------------------------'}
      {System.show ''}
      {System.show ' writing: do/c re/d mi/e fa/f sol/g'}
      {Write ['do'#c re#d mi#e fa#f sol#g] P paxos commit}
      {System.show ' outcome: '#{WaitStream S 1}}
      {System.show ' Reading one, majority and all'}
      %{FullGetKeys ['do' re mi fa sol]}
      {FullGetKeys ['do']}
      {System.show '---------------------------------'}
      {System.show ' testing the rset '}
      {MasterOfPuppets findRSet(RSetFlag)}
      {Wait RSetFlag}
      
      /*{System.show ' aborting writes '}
      {Write ['do'#dodo re#rere mi#mimi fa#fafa sol#solsol] P paxos abort}
      {System.show ' outcome: '#{WaitStream S 2}}
      {System.show ' Reading one, majority and all'}
      {FullGetKeys ['do' re mi fa sol]}
      {System.show '---------------------------------'}
      {System.show ' committing writes '}
      {Write ['do'#dodo re#rere mi#mimi fa#fafa sol#solsol] P paxos commit}
      {System.show ' outcome: '#{WaitStream S 3}}
      {System.show ' Reading one, majority and all'}
      {FullGetKeys ['do' re mi fa sol]}
      {System.show '---------------------------------'}
      {System.show ' testing the rset '}
      {MasterOfPuppets findRSet(RSetFlag)}
      {Wait RSetFlag}
      {System.show ' RSet ready'}
      {System.show ' going to write values'}
      {Write [norte#north sur#south este#east oeste#west] P paxos commit}
      {System.show ' outcome: '#{WaitStream S 4}}
      {System.show ' Reading one, majority and all'}
      {FullGetKeys [norte sur este oeste]}*/
   end
   {Application.exit 0}
end
