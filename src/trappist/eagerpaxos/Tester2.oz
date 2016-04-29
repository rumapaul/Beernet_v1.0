%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   Property
   System
   PbeerMaker     at '../../pbeer/Pbeer.ozf'
   Random         at '../../utils/Random.ozf'

define
   SIZE  = 20

   MasterOfPuppets
   NetRef
   AlivePbeers
   ExpiredTrId
   ChangedLock

   proc {ReceivingLoop P PbeerId}
       NewMsg
       in
       {P receive(NewMsg)}
       {Wait NewMsg}
       case NewMsg 
       of suicide then
          skip
       [] isolation then
          {System.showInfo "Received Isolation:"#PbeerId}
          {ReceivingLoop P PbeerId}
       [] joinack then
          {System.showInfo "Received JoinAck:"#PbeerId}
          {ReceivingLoop P PbeerId}
       [] lockExpire(_#TrId#_#_) then
          {System.showInfo "Lock has Expired!!"}
          ExpiredTrId = TrId
          {ReceivingLoop P PbeerId}
       [] update(K V) then
          {System.showInfo "Peer "#PbeerId#" has received update for key "#K#" value:"#V}
          {ReceivingLoop P PbeerId}
       [] lockChange(_#TrId#_#_ NewPeerTM#_#NewTM#TrPeriod) then
          {System.show ' Received Lock Change message!! '}
          ChangedLock = lockstat(tmpeerid:NewPeerTM trid:TrId newtmid:NewTM period:TrPeriod)
          {ReceivingLoop P PbeerId}
       else
          {ReceivingLoop P PbeerId}
       end
   end

   proc {CreateNetwork}
      MasterId
      proc {CreateAPbeer N}
         if N > 0 then
             Pbeer PbeerId
             in
             Pbeer = {PbeerMaker.new args}
             {Pbeer join(NetRef)}
             PbeerId = {Pbeer getId($)}
             thread {ReceivingLoop Pbeer PbeerId} end  
             AlivePbeers.PbeerId:=Pbeer
             {CreateAPbeer N-1}
         end
      end
      in 
      
      MasterOfPuppets := {PbeerMaker.new args(firstAck:unit)}
      MasterId = {@MasterOfPuppets getId($)}
      thread {ReceivingLoop @MasterOfPuppets MasterId} end
      AlivePbeers.MasterId:=@MasterOfPuppets
      NetRef = {@MasterOfPuppets getFullRef($)}
      {CreateAPbeer SIZE-1}
   end

   proc {GetOne Key}
      Value
   in
      %% TODO: remove trapp and make getOne more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {@MasterOfPuppets getOne(Key Value trapp)}
      {Wait Value}
      {System.show 'Getting one replica of '#Key#' we obtained '#Value}
   end

   proc {GetAll Key}
      Val
   in
      %% TODO: remove trapp and make getAll more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {@MasterOfPuppets getAll(Key Val trapp)}
      if {IsList Val} then skip end
      %{Wait Val}
      {System.show 'Reading All '#Key#' we obtained '#Val}
   end

   proc {GetMajority Key}
      Val
   in
      %% TODO: remove trapp and make getMajority more transparent.
      %% e.g. make it pass first to trappist and not directly to Replica
      {@MasterOfPuppets getMajority(Key Val trapp)}
      if {IsList Val} then skip end
      %{Wait Val}
      {System.show 'Reading Majority '#Key#' we obtained '#Val}
   end

   proc {Write Pairs Client Protocol Action}
      proc {Trans TM}
         for Key#Value in Pairs do
            {TM write(Key Value)}
         end
         {TM Action}
      end
   in
      {@MasterOfPuppets runTransaction(Trans Client Protocol)}
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

   MasterOfPuppets = {NewCell nil}
   AlivePbeers = {Dictionary.new}
   {CreateNetwork}
   {System.show 'network created. Going to do transactions'}
   
   local
      P S RSetFlag LockKey1 LockKey3 LockKey5
   in
      P = {NewPort S}
      {System.show ' testing the rset '}
      {@MasterOfPuppets findRSet(RSetFlag)}
      {Wait RSetFlag}
      {System.show '----- Trying out transactions with Eager Paxos -----'}
      {System.show '-----------------------------------'}
      {System.show ''}
      {System.show ' writing: do/c re/d mi/e fa/f sol/g'}
      {Write ['do'#c re#d mi#e fa#f sol#g] P paxos commit}
      {System.show ' outcome: '#{WaitStream S 1}}
      {FullGetKeys ['do' re mi fa sol]}
      local
         AllKeys = {Dictionary.keys AlivePbeers}
         ChosenKey = {List.nth AllKeys {Random.urandInt 1 {List.length AllKeys}}}
         Pbeer = {Dictionary.condGet AlivePbeers ChosenKey nil}
         in
     	 if Pbeer\=nil then
            Result
            in
            {System.showInfo "Making Peer "#ChosenKey#" reader of mi"}
            {Pbeer becomeReader(mi Result)}
            {Wait Result}
            if Result == success then
               {System.showInfo "Peer "#ChosenKey#" successfully became reader of mi!!"}
            else
    	       {System.showInfo "Peer "#ChosenKey#" failed to become reader of mi!!"}
            end
         end
      end
      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks(['do' re mi fa sol] LockKey1)}
      {Wait LockKey1}
      case LockKey1
      of 'error' then
         {System.show ' Failed to get Lock of the keys'}
      else
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {@MasterOfPuppets commitTransaction(LockKey1 ['do'#dodo re#rere mi#mimi fa#fafa sol#solsol])}
         {Delay 10000}
         {FullGetKeys ['do' re mi fa sol]}
      end
      {System.show '---------------------------------'}

      local
         AllKeys = {Dictionary.keys AlivePbeers}
         ChosenKey = {List.nth AllKeys {Random.urandInt 1 {List.length AllKeys}}}
         Pbeer = {Dictionary.condGet AlivePbeers ChosenKey nil}
         in
     	 if Pbeer\=nil then
            Result
            in
            {System.showInfo "Making Peer "#ChosenKey#" reader of francais"}
            {Pbeer becomeReader(francais Result)}
            {Wait Result}
            if Result == success then
               {System.showInfo "Peer "#ChosenKey#" successfully became reader of francais!!"}
            else
    	       {System.showInfo "Peer "#ChosenKey#" failed to become reader of francais!!"}
            end
         end
      end

      {System.show ' writing (using eager-paxos): francais/french espanol/spanish anglais/english chinois/chinese'}
      {@MasterOfPuppets getLocks([francais espanol anglais chinois] LockKey3)}
      {Wait LockKey3}
      case LockKey3
      of 'error' then
         {System.show ' Failed to get Lock of the keys'}
      else
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {@MasterOfPuppets commitTransaction(LockKey3 [francais#french espanol#spanish anglais#english chinois#chinese])}
         {Delay 10000}
         {FullGetKeys [francais espanol anglais chinois]}
      end
      {System.show '---------------------------------'}

      local
         AllKeys = {Dictionary.keys AlivePbeers}
         ChosenKey = {List.nth AllKeys {Random.urandInt 1 {List.length AllKeys}}}
         Pbeer = {Dictionary.condGet AlivePbeers ChosenKey nil}
         in
     	 if Pbeer\=nil then
            Result
            in
            {System.showInfo "Making Peer "#ChosenKey#" reader of francais"}
            {Pbeer becomeReader(francais Result)}
            {Wait Result}
            if Result == success then
               {System.showInfo "Peer "#ChosenKey#" successfully became reader of francais!!"}
            else
    	       {System.showInfo "Peer "#ChosenKey#" failed to become reader of francais!!"}
            end
         end
      end

      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks([francais espanol anglais chinois] LockKey5)}
      {Wait LockKey5}
      case LockKey5
      of 'error' then
         {System.show ' Failed to get Lock of the keys'}
      else
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {@MasterOfPuppets commitTransaction(LockKey5 [francais#thefrench espanol#thespanish anglais#theenglish chinois#thechinese])}
         {Delay 10000}
         {FullGetKeys [francais espanol anglais chinois]}
      end
      {System.show '---------------------------------'}
   end
   {Application.exit 0}
end
