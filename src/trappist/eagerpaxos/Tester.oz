%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   Property
   System
   %Network        at '../../network/Network.ozf'
   PbeerMaker     at '../../pbeer/Pbeer.ozf'
   %Utils          at '../../utils/Misc.ozf'

define
   SIZE  = 20

   MasterOfPuppets
   %MaxKey
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

  proc {KillMaster}
     CandidatePuppet 
     in
     CandidatePuppet = {@MasterOfPuppets getSucc($)}
     if CandidatePuppet\=nil andthen CandidatePuppet.id\={@MasterOfPuppets getId($)} then
	CandidateMasterPbeer = {Dictionary.condGet AlivePbeers CandidatePuppet.id nil}
        in
        if CandidateMasterPbeer \= nil then
	   Pbeer = @MasterOfPuppets
           in
           MasterOfPuppets:=CandidateMasterPbeer
           {Pbeer injectPermFail}
        end
     end
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
   %MaxKey = {MasterOfPuppets getMaxKey($)}
   {System.show 'network created. Going to do transactions'}
   
   local
      P S RSetFlag LockKey1 LockKey2 LockKey3 LockKey4 LockKey5 LockKey6
      TrPeriod
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
         {Delay 30000}
         {FullGetKeys ['do' re mi fa sol]}
      end
      {System.show '---------------------------------'}
      
      {System.show ' aborting writes '}
      {Write [norte#north sur#south este#east oeste#west] P paxos commit}
      {System.show ' outcome: '#{WaitStream S 2}}
      {System.show ' Reading one, majority and all'}
      {FullGetKeys [norte sur este oeste]}
      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks(['do' re mi fa sol] LockKey2)}
      {Wait LockKey2}
      case LockKey2
      of 'error' then
         {System.show ' Failed to get Lock of the keys'}
      else
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {@MasterOfPuppets abortTransaction(LockKey2)}
         {Delay 10000}
      end
      {FullGetKeys ['do' re mi fa sol]}
      {System.show '---------------------------------'}

      /*{System.show ' writing (using eager-paxos): francais/french espanol/spanish anglais/english chinois/chinese'}
      {@MasterOfPuppets getLocks([francais espanol anglais chinois] LockKey3)}
      {Wait LockKey3}
      case LockKey3
      of 'error' then
         {System.show ' Failed to get Lock of the keys'}
      else
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {@MasterOfPuppets commitTransaction(LockKey3 [francais#french espanol#spanish anglais#english chinois#chinese])}
         {Delay 30000}
         {FullGetKeys [francais espanol anglais chinois]}
      end
      {System.show '---------------------------------'}

      {System.show ' Expiring Lock '}
      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks([norte sur este oeste] LockKey4)}
      {Wait LockKey4}
      case LockKey4
      of 'error' then
	 {System.show ' Failed to get Lock of the keys'}
      [] _#TrId#_#P then
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         TrPeriod = P
         {Delay TrPeriod+10000}
         {Wait ExpiredTrId}
         if ExpiredTrId == TrId then
           {System.showInfo "Expired Tr Id matched!!"}
         end
      end
      {System.show '---------------------------------'}

      {System.show 'Transaction Period:'#TrPeriod}

      {System.show ' Killing TM after getting lock '}
      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks([norte sur este oeste] LockKey5)}
      {Wait LockKey5}
      case LockKey5
      of 'error' then
	 {System.show ' Failed to get Lock of the keys'}
      [] _#TrId#_#_ then
         {Delay 10000}
         {System.show ' Got lock of the keys!!'}
         {KillMaster}
         {Delay TrPeriod+10000}
         if {Not {Value.isFree ChangedLock}} andthen ChangedLock.trid == TrId then
            {System.show 'Received New Lock after TM crash!!'}
            {@MasterOfPuppets commitTransaction(ChangedLock.tmpeerid#ChangedLock.trid#ChangedLock.newtmid#ChangedLock.period
                                               [norte#northern sur#southern este#eastern oeste#western])}
         end
         {FullGetKeys [norte sur este oeste]}
      end
      {System.show '---------------------------------'}
      

      {System.show ' Killing TM before getting lock '}
      {System.show ' Get Locks of all'}
      {@MasterOfPuppets getLocks([norte sur este oeste] LockKey6)}
      {Delay 150}
      thread {KillMaster} end
      {System.show 'Killed Master!!'}
      {Delay TrPeriod+10000}
      if {Value.isFree LockKey6} then
         {System.show ' Failed to get Lock of the keys'}
      else
      	case LockKey6
      	of 'error' then
	 	{System.show ' Failed to get Lock of the keys'}
      	[] _#_#_#_ then
         	{Delay 10000}
         	{System.show ' Got lock of the keys!!'}
         	{@MasterOfPuppets commitTransaction(LockKey6 [norte#northnorth sur#southsouth 
			este#easteast oeste#westwest])}
         	{Delay 10000}
      	end
      end
      {System.show '---------------------------------'}
      {FullGetKeys [norte sur este oeste]}*/
   end
   {Application.exit 0}
end
