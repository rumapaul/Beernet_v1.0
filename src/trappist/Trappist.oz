/*-------------------------------------------------------------------------
 *
 * Trappist.oz
 *
 *    Interface to the different strategies for transactional storage
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
 *    Last change: $Revision: 514 $ $Author: ruma $
 *
 *    $Date: 2016-04-15 14:44:27 +0200 (Fri, 15 April 2016) $
 *
 * NOTES
 *    
 *    Pre-condition: It needs a messaging layer, the DHT component, a
 *    replication manager and the Node Reference
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component      at '../corecomp/Component.ozf'
   Constants      at '../commons/Constants.ozf'
   Utils          at '../utils/Misc.ozf'
   EagerPaxosTM   at 'eagerpaxos/PaxosEager-TM.ozf'
   EagerPaxosTP   at 'eagerpaxos/PaxosEager-TP.ozf'
   PaxosTM        at 'paxos/Paxos-TM.ozf'
   PaxosTP        at 'paxos/Paxos-TP.ozf'
   TwoPhaseTM     at 'twophase/TwoPhase-TM.ozf'
   TwoPhaseTP     at 'twophase/TwoPhase-TP.ozf'
   ValueSetTM     at 'valueset/ValueSet-TM.ozf'
   ValueSetTP     at 'valueset/ValueSet-TP.ozf'
   Timer          at '../timer/Timer.ozf'
export
   New
define

   %Debug = Utils.blabla

   fun {New CallArgs}
      Self
      Listener
      Suicide
      MsgLayer
      NodeRef
      Replica

      %Timeout
      TMs
      TPs
      DBMan
      PairsDB
      SetsDB
      MaxKey

      TheTimer

      TMmakers = tms(eagerpaxos: EagerPaxosTM
                     paxos:      PaxosTM
                     twophase:   TwoPhaseTM
                     valueset:   ValueSetTM
                     )
      TPmakers = tps(eagerpaxos: EagerPaxosTP
                     paxos:      PaxosTP
                     twophase:   TwoPhaseTP
                     valueset:   ValueSetTP
                     )

      DBs      = dbs(eagerpaxos: PairsDB
                     paxos:      PairsDB
                     twophase:   PairsDB
                     valueset:   SetsDB
                     )

      proc {AddTransObj TransDict Tid ObjId Obj}
         TheObjs
      in
         TheObjs = {Dictionary.condGet TransDict Tid objs}
         TransDict.Tid := {Record.adjoinAt TheObjs ObjId Obj}
      end

      proc {RemoveTransObj TransDict Tid ObjId}
         TheObjs
      in
         TheObjs = {Dictionary.condGet TransDict Tid objs}
         %{TheObjs.ObjId signalDestroy}
         TransDict.Tid := {Record.subtract TheObjs ObjId}
         if {Record.width TransDict.Tid}==0 then
	    {Dictionary.remove TransDict Tid}	
         end
      end

      proc {ApplyEventAllTransObj TransDict Event}	%% RRP
	AllEntries = {Dictionary.entries TransDict}
        in 
	{List.forAll AllEntries
            proc {$ _#I}
	       {Record.forAll I 
			proc {$ Obj} 
			    {Obj Event} 
                        end}
            end}
      end

      %% === Events =========================================================

      %% --- Trappist API ---------------------------------------------------

      proc {BecomeReader becomeReader(Key Result)}
         TM
         P S
         in
         P = {NewPort S}
         TM = {TMmakers.paxos.new args(role:leader
                                       client:P
                                       maxKey:@MaxKey)}
         {TM setMsgLayer(@MsgLayer)}
         {TM setReplica(@Replica)}
         {TM setListener(Self)}
         {AddTransObj TMs {TM getTid($)} {TM getId($)} TM}
         thread
         	{TM becomeReader(k:Key)}
         	{Wait S.1}
                case S.1
         	of subscribed then
            		Result = success
         	[] failed then
            		Result = not_success
		end
         end
      end

      proc {GetLocks getLocks(Keys LockId)}
         ATM Cap
         P S
         in
         P = {NewPort S}
         Cap = {Name.new}
         ATM = {TMmakers.eagerpaxos.new args(role:leader
                                       client:P
                                       maxKey:@MaxKey
                                       transId:Cap)}
         
         {ATM setMsgLayer(@MsgLayer)}
         {ATM setReplica(@Replica)}
         {ATM setListener(Self)}
         {AddTransObj TMs Cap {ATM getId($)} ATM}
         thread
         	{ATM getLocks(Keys)}
         	case S.1
         		of granted#TMPeerId#TrId#TMId#Period then
            		LockId = TMPeerId#TrId#TMId#Period 
         	[] abort#_#_#_#_ then
            		LockId = error
		end
         end
      end

      proc {CommitTransaction commitTransaction(TMPeerId#TransId#TMId#Period KeyValuePairs)}
         if @NodeRef.id == TMPeerId then
         	TMObj
         	in
         	TMObj = {Dictionary.condGet TMs TransId objs}
         	if {HasFeature TMObj TMId} then
               	 	{TMs.TransId.TMId commitTransaction(KeyValuePairs)}
         	end
         else
                {@MsgLayer send(commitTransaction(TMPeerId#TransId#TMId#Period KeyValuePairs) to:TMPeerId)}
         end
      end

      proc {AbortTransaction abortTransaction(TMPeerId#TransId#TMId#Period)}
         if @NodeRef.id == TMPeerId then
         	TMObj
         	in
         	TMObj = {Dictionary.condGet TMs TransId objs}
         	if {HasFeature TMObj TMId} then
                	{TMs.TransId.TMId abort}
         	end
	else
		{@MsgLayer send(abortTransaction(TMPeerId#TransId#TMId#Period) to:TMPeerId)}
        end
      end

      %% Just a forward to runTransaction.
      %% Keeps backward compatibility
      proc {ExecuteTransaction executeTransaction(Trans Client Protocol)}
         {RunTransaction runTransaction(Trans Client Protocol)}
      end

      proc {RunTransaction runTransaction(Trans Client Protocol)}
         TM
      in
         TM = {TMmakers.Protocol.new args(role:leader
                                          client:Client
                                          maxKey:@MaxKey)}
         {TM setMsgLayer(@MsgLayer)}
         {TM setReplica(@Replica)}
         {TM setListener(Self)}
         {AddTransObj TMs {TM getTid($)} {TM getId($)} TM}
         {Trans TM}
      end

      %% --- Trappist API for Key/Value-Sets --------------------------------
      %% Slightly different that Run Transaction
      %% Event can be:
      %% add(k:SetKey s:SetSecret v:Value sv:ValueSecret c:ClientP)
      %% remove(k:SetKey s:SetSecret v:Value sv:ValueSecret c:ClientP)
      %% readSet(k:SetKey v:Value)
      proc {ToValueSet Event}
         TM 
      in
         TM = {TMmakers.valueset.new args(role:leader maxKey:@MaxKey)}
         {TM setMsgLayer(@MsgLayer)}
         {TM setReplica(@Replica)}
         {AddTransObj TMs {TM getTid($)} {TM getId($)} TM}
         {TM Event}
      end

      %% --- For the TMs ----------------------------------------------------
      proc {DeleteTM Event}
         {RemoveTransObj TMs Event.tid Event.tmid}
      end

      proc {DestroyRTM Event}
         {ForwardToTM Event}
         {RemoveTransObj TMs Event.tid Event.tmid} 
      end

      proc {InitRTM Event}
         Client = Event.client
         Protocol = Event.protocol
         Tid = Event.tid
         RTM
      in
         RTM = {TMmakers.Protocol.new args(role:rtm
                                           client:Client
                                           maxKey:@MaxKey)}
         {RTM setMsgLayer(@MsgLayer)}
         {RTM setReplica(@Replica)}
         {RTM setListener(Self)}
         {AddTransObj TMs Tid {RTM getId($)} RTM}
	 {@Listener monitor(Event.leader.ref priority:true)}
         {RTM Event}
      end

      proc {ForwardToTM Event}
         TMObj
         in
         TMObj = {Dictionary.condGet TMs (Event.tid) objs}
         if {HasFeature TMObj (Event.tmid)} then
         	{TMs.(Event.tid).(Event.tmid) Event}
         end
      end 

      proc {MonitorRTMs Event}
        RTMSet = Event.1
        in
	for TM in RTMSet do
            if TM.ref.id \= @NodeRef.id then
            	{@Listener monitor(TM.ref priority:true)}
            end
        end
        {ForwardToTM Event}
      end

      proc {RegisterRTM Event}
        {@Listener monitor(Event.rtm.ref priority:true)}
  	{ForwardToTM Event}
      end

      proc {AskRTMResponse Event}
         TMObj
         in
         TMObj = {Dictionary.condGet TMs (Event.tid) objs}
         {Record.forAll TMObj 
			proc {$ Obj} 
			    {Obj Event} 
                        end}
      end

      proc {NotifyReaderUpdate notifyReaderUpdate(key:K val:V tag:trapp)}
         {@Listener clientEvents(msg:update(K V))}
      end

      proc {NotifyReaderLocking notifyReaderLocking(key:K tag:trapp)}
         {@Listener clientEvents(msg:locked(K))}
      end

      proc {NotifyLockChange notifyLockChange(old:OldLock new:NewLock tag:trapp)}
         {@Listener clientEvents(msg:lockChange(OldLock NewLock))}
      end

      proc {NotifyLockExpire notifyLockRelease(lockKey:LockId tag:trapp)}
	{@Listener clientEvents(msg:lockExpire(LockId))}
      end

      %% --- For the TPs ----------------------------------------------------
      proc {Brew Event}
         Tid = Event.tid
         Protocol = Event.protocol
         TP
      in
         TP = {TPmakers.Protocol.new args(tid:Tid)} 
         {TP setMsgLayer(@MsgLayer)}
         {TP setDB(DBs.Protocol)}
         {AddTransObj TPs Tid {TP getId($)} TP}
         {TP Event}
      end

      proc {Final Event}
         TPObj
         in
         TPObj = {Dictionary.condGet TPs (Event.tid) objs}
         if {HasFeature TPObj (Event.tpid)} then
         	{TPs.(Event.tid).(Event.tpid) Event.decision}
                {RemoveTransObj TPs Event.tid Event.tpid}
         end
      end

      proc {Update Event}
         Tid = Event.tid
         Protocol = Event.protocol
         TP
         in
         TP = {TPmakers.Protocol.new args(tid:Tid)} 
         {TP setMsgLayer(@MsgLayer)}
         {TP setDB(DBs.Protocol)}
         %{AddTransObj TPs Tid {TP getId($)} TP}
         {TP Event}
      end

      proc {ReleaseLock Event}
	 Tid = Event.tid
         Protocol = Event.protocol
         TP
         in
         TP = {TPmakers.Protocol.new args(tid:Tid)} 
         {TP setMsgLayer(@MsgLayer)}
         {TP setDB(DBs.Protocol)}
         {TP Event}
      end

      proc {LeaderChanged Event}
         TPObj
         in
         TPObj = {Dictionary.condGet TPs (Event.tid) objs}
         if {HasFeature TPObj (Event.tpid)} then
            {TPObj.(Event.tpid) leaderChanged}
            {RemoveTransObj TPs Event.tid Event.tpid}
         end
      end

      proc {NewReader Event}
         Tid = Event.tid
         Protocol = Event.protocol
         TP
         in
         TP = {TPmakers.Protocol.new args(tid:Tid)} 
         {TP setMsgLayer(@MsgLayer)}
         {TP setDB(DBs.Protocol)}
         {TP Event}
      end

      %% --- Data Management ------------------------------------------------
      proc {NewPred newPred(old:OldPred new:NewPred tag:data)}
         proc {DataLoop Froms Tos}
            case Froms#Tos
            of (From|MoreFroms)#(To|MoreTos) then
               PairEntries SetEntries
            in
               %% Migrating data of paxos, eagerpaxos and twophase
               %% They all use PairsDB, refering with DBs.paxos
               {DBs.paxos dumpRange(From To PairEntries)}
	       if PairEntries\=nil then
               	{@MsgLayer dsend(to:NewPred insertData(entries:PairEntries
                                                      db:paxos
                                                      tag:trapp))}
               end
               %% Migrating data of the valueset abstraction
               {DBs.valueset dumpRange(From To SetEntries)}
               if SetEntries\=nil then
               	{@MsgLayer dsend(to:NewPred insertData(entries:SetEntries
                                                      db:valueset
                                                      tag:trapp))}
               end
               %% Carry on with the migration
               {DataLoop MoreFroms MoreTos}
            [] nil#nil then
               skip
            end
         end
         FromList
         ToList
      in
         thread
            %% Safe thread. Does not modify state
            FromList = OldPred.id|{@Replica getReverseKeys(OldPred.id $)}
            ToList   = NewPred.id|{@Replica getReverseKeys(NewPred.id $)}
            {DataLoop FromList ToList}
          end
      end

      proc {NewResponsibilities newResponsibilities(old:OldPredId new:NewPredId tag:trapp)}
	proc {DataLoop Tos}
            case Tos
            of (To|MoreTos) then
               {@MsgLayer send(doMigration(left:NewPredId right:OldPredId dest:@NodeRef hkey:To tag:trapp) to:To)}
               %% Carry on with the migration
               {DataLoop MoreTos}
            [] nil then
               skip
            end
         end
         OldReplicaList
         PairEntries SetEntries
         in
         thread
         	{DBs.paxos dumpRange(NewPredId OldPredId PairEntries)}
	 	{DBs.valueset dumpRange(NewPredId OldPredId SetEntries)}
	 	if PairEntries==nil andthen SetEntries == nil then % Check Data staleness from timestamp
			OldReplicaList = {@Replica getReverseKeys(OldPredId $)}
         		{DataLoop OldReplicaList}
         	else
               		skip  %% change data ownership buy deleting tag and timestap from data item
         	end
	 end
	 %skip
      end

      proc {DoMigration doMigration(left:FromId right:ToId dest:Dest hkey:_ tag:trapp)}
           proc {DataLoop Froms Tos}
            case Froms#Tos
            of (From|MoreFroms)#(To|MoreTos) then
               PairEntries SetEntries
            in
               %% Migrating data of paxos, eagerpaxos and twophase
               %% They all use PairsDB, refering with DBs.paxos
               {DBs.paxos dumpRange(From To PairEntries)}
	       if PairEntries\=nil then
               	{@MsgLayer dsend(to:Dest insertDataAttempt(entries:PairEntries
                                                      db:paxos
                                                      tag:trapp))}
	       end
               %% Migrating data of the valueset abstraction
               {DBs.valueset dumpRange(From To SetEntries)}
	       if SetEntries\=nil then	
               	{@MsgLayer dsend(to:Dest insertDataAttempt(entries:SetEntries
                                                      db:valueset
                                                      tag:trapp))}
               end
               %% Carry on with the migration
               {DataLoop MoreFroms MoreTos}
            [] nil#nil then
               skip
            end
         end
         FromList
         ToList
      in
         thread
            %% Safe thread. Does not modify state
            FromList = FromId|{@Replica getReverseKeys(FromId $)}
            ToList   = ToId|{@Replica getReverseKeys(ToId $)}
            {DataLoop FromList ToList}
          end
      end

      proc {InsertData insertData(entries:Entries db:DB tag:trapp)}
         %if Entries \= nil then
         %   {DBs.DB insert(Entries _/*Result*/)}
         %end
         if Entries \= nil then
            {DBs.DB insertNewest(Entries _/*Result*/)}
         end
      end

      proc {InsertDataAttempt insertDataAttempt(entries:Entries db:DB tag:trapp)}
         if Entries \= nil then
            {DBs.DB insertNewest(Entries _/*Result*/)}
         end
      end

      proc {UseSuccBackUp Event}
	PairEntries SetEntries
        MyPred MySucc
        in
        MyPred = {@Listener getPred($)}
        MySucc = {@Listener getSucc($)}
        %% Taking backup of data of paxos, eagerpaxos and twophase
        %% They all use PairsDB, refering with DBs.paxos
        {DBs.paxos dumpRange(MyPred.id @NodeRef.id PairEntries)}

        %% Migrating data of the valueset abstraction
        {DBs.valueset dumpRange(MyPred.id @NodeRef.id SetEntries)}
	if PairEntries\=nil orelse SetEntries \= nil then
        	{@MsgLayer dsend(to:MySucc requestDataBackUp(requester:@NodeRef
							pairentries:PairEntries
                                                      	setentries:SetEntries
							tag:trapp))}
        end
      end

      proc {RequestDataBackUp requestDataBackUp(requester:Pbeer 
						pairentries:PairEntries
						setentries:SetEntries
						tag:trapp)}
	MyPred = {@Listener getPred($)}
	in
	if Pbeer.id \= @NodeRef.id andthen Pbeer.id==MyPred.id then
		% Need to check whether the requester is my pred
 		if PairEntries \= nil then
            		{DBs.paxos insertBackUpData(PairEntries _/*Result*/)}
		end

               if SetEntries \= nil then
			{DBs.valueset insertBackUpData(SetEntries _/*Result*/)}
	       end
	end
      end

      %% --- Internal to the Pbeer ---
      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
      end

      proc {SetReplica setReplica(ReplicaMan)}
         Replica := ReplicaMan
      end

      %proc {SetTimeout setTimeout(ATime)}
      %   Timeout := ATime
      %end

      proc {HandleNodeCrash nodeCrash(node:Pbeer tag:trapp)}
        if Pbeer.id \= @NodeRef.id then
                thread
			{ApplyEventAllTransObj TMs isATMCrashed(Pbeer)}
		end
        end
      end

      proc {SignalDestroy Event}
        {ApplyEventAllTransObj TMs Event}
        {ApplyEventAllTransObj TPs Event}
        {Suicide}
      end

      Events = events(
                     %% Trappist's API
                     becomeReader:  BecomeReader
                     executeTransaction:ExecuteTransaction
                     getLocks:      GetLocks
                     runTransaction:RunTransaction
                     commitTransaction: CommitTransaction
                     abortTransaction:  AbortTransaction
                     %% Directly to Key/Value-Sets
                     add:           ToValueSet
                     remove:        ToValueSet
                     readSet:       ToValueSet
                     createSet:     ToValueSet
                     destroySet:    ToValueSet
                     %% For the TMs
                     ack:           ForwardToTM
                     initRTM:       InitRTM
                     registerRTM:   RegisterRTM
                     rtms:          MonitorRTMs
                     startLeader:   ForwardToTM
                     stopLeader:    ForwardToTM
                     okLeader:      ForwardToTM
                     setFinal:      ForwardToTM
                     updateRTM:     ForwardToTM
                     notifyTermination: ForwardToTM
                     vote:          ForwardToTM
                     voteAck:       ForwardToTM
                     deleteTM:	    DeleteTM
                     destroyRTM:    DestroyRTM
                     askRTMResponse: AskRTMResponse
                     aRTMResponse:  ForwardToTM
                     ackNewReader:  ForwardToTM
                     notifyReaderUpdate: NotifyReaderUpdate
             	     notifyReaderLocking: NotifyReaderLocking
                     notifyLockChange:    NotifyLockChange
	             notifyLockRelease:   NotifyLockExpire
                     %% For the TPs
                     brew:          Brew
                     final:         Final
                     leaderChanged: LeaderChanged
                     newReader:     NewReader
                     update:        Update
                     releaseLock:   ReleaseLock
                     %% Data management
                     insertData:    InsertData
                     insertDataAttempt: InsertDataAttempt
                     doMigration:   DoMigration
                     newPred:       NewPred
                     newResponsibilities: NewResponsibilities
		     useSuccBackUp:	UseSuccBackUp
		     requestDataBackUp: RequestDataBackUp
                     %% Internal to the Pbeer
                     setMsgLayer:   SetMsgLayer
                     setReplica:    SetReplica
                     %setTimeout:    SetTimeout
                     %timeout:       TimeoutEvent
		     nodeCrash:	     HandleNodeCrash
                     signalDestroy:  SignalDestroy
                     )

   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Listener = FullComponent.listener
         Suicide = FullComponent.killer
      end
      NodeRef  = {NewCell noref}
      MsgLayer = {NewCell Component.dummy}
      Replica  = {NewCell Component.dummy}

      TheTimer    = {Timer.new}
      {TheTimer setListener(Self)}

      TMs      = {Dictionary.new}
      TPs      = {Dictionary.new}

      local
         Args     = {Utils.addDefaults CallArgs def(maxKey:Constants.largeKey)}
      in
         DBMan    = Args.dbman
         MaxKey   = {NewCell Args.maxKey}
      end
      PairsDB  = {DBMan getCreate(name:trapp type:basic db:$)}
      SetsDB   = {DBMan getCreate(name:sets type:secrets db:$)}

      Self
   end

end  

