/*-------------------------------------------------------------------------
 *
 * RelaxedRing.oz
 *
 *    Relaxed-ring maintenance algorithm
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *	      Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 512 $ $Author: ruma $
 *
 *    $Date: 2016-04-28 13:08:08 +0200 (Thurs, 28 April 2016) $
 *
 * NOTES
 *      
 *    Join and failure recovery are implemented here.
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
   System
   BootTime at 'x-oz://boot/Time'
   Component   at '../../corecomp/Component.ozf'
   Constants   at '../../commons/Constants.ozf'
   KeyRanges   at '../../utils/KeyRanges.ozf'
   Random      at '../../utils/Random.ozf'   
   Network     at '../../network/Network.ozf'
   PbeerList   at '../../utils/PbeerList.ozf'
   RingList    at '../../utils/RingList.ozf'
   TimerMaker  at '../../timer/Timer.ozf'
   Utils       at '../../utils/Misc.ozf'
   PeriodicStabilizer at 'PeriodicStabilizer.ozf'
   PeriodicPhaseCalculator at 'PeriodicPhaseCalculator.ozf'
   Merger      at 'Merger.ozf'

export
   New
define
   JOIN_WAIT   = Constants.rlxRingJoinWait      % Milliseconds to wait to retry a join 
   MAX_KEY     = Constants.largeKey
   SL_SIZE     = Constants.slSize
   IS_VISUAL_DEBUG = Constants.isVisual
   ATTEMPT_TO_JOIN = Constants.rlxRingAttemptToJoin   % After these many attempt node will notify upper layer

   BelongsTo      = KeyRanges.belongsTo

   %% --- Utils ---
   fun {Vacuum L Dust}
      case Dust
      of DeadPeer|MoreDust then
         {Vacuum {RingList.remove DeadPeer L} MoreDust}
      [] nil then
         L
      end
   end

   %% --- Exported ---
   fun {New CallArgs}
      Crashed     % List of crashed peers
      MaxKey      % Maximum value for a key
      Pred        % Reference to the predecessor
      PredList    % To remember peers that haven't acked joins of new preds
      Ring        % Ring Reference ring(name:<atom> id:<name>)
      FingerTable % Routing table 
      SelfStabilizer  % Periodic Stalizer
      SelfMerger  % Merge Module
      Self        % Full Component
      SelfRef     % Pbeer reference pbeer(id:<Id> port:<Port>)
      Succ        % Reference to the successor
      SuccList    % Successor List. Used for failure recovery
      SLSize      % Successor list size
      WishedRing  % Used while trying to join a ring
      FailedAttempt % Used to take a decision when totally isolated
      KnowledgeBase % Knowledge about other nodes on the ring, used for failure recovery
      Phase       % Current Phase
      SelfPhaseComputer % Periodic Phase Calculation
      CurrentPhase % Last computed phase

      %% --- Utils ---
      ComLayer    % Network component
      Listener    % Component where the deliver messages will be triggered
      Logger      % Component to log every sent and received message  (R)
      Timer       % Component to rigger some events after the requested time
      Suicide     

      Args
      FirstAck    % One shoot acknowledgement for first join
      CurAck  

      % ------Maintainence Flags -----
      PSFlag
      MergerFlag
      KBFlag 

      fun {AddToList Peer L}
         {RingList.add Peer L @SelfRef.id MaxKey}
      end

      %% TheList should have no more than Size elements
      fun {UpdateList MyList NewElem OtherList}
         FinalList _/*DropList*/
      in
         FinalList = {NewCell MyList}
         {RingList.forAll {Vacuum {AddToList NewElem OtherList} @Crashed}
                           proc {$ Pbeer}
                              FinalList := {AddToList Pbeer @FinalList}
                           end}
         FinalList := {RingList.keepAndDrop SLSize @FinalList _/*DropList*/}
         % TODO: verify this operation
         %{UnregisterPeers DropList}
         {WatchPeers @FinalList}
         @FinalList
      end

      proc {BasicForward Event}
         case Event
         of route(msg:Msg src:_ to:_) then
            if @Succ \= nil then
               {Zend @Succ Msg}
            end
         else
            skip
         end
      end

      proc {Backward Event Target}
         ThePred = {RingList.getAfter Target @PredList @SelfRef.id MaxKey}
      in
         if ThePred \= nil then
            {Zend ThePred {Record.adjoinAt Event branch true}}
         end
      end

      %% Registering a Pbeer on the failure detector
      proc {Monitor Pbeer Priority}
         {@ComLayer monitor(Pbeer priority:Priority)}
      end

      proc {RlxRoute Event Target}
         if {HasFeature Event last} andthen Event.last then
            %% I am supposed to be the responsible, but I have a branch
            %% or somebody was missed (non-transitive connections)
            {Backward Event Target}
         elseif {BelongsTo Event.src.id @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible => set last = true
            {Zend @Succ {Record.adjoinAt Event last true}}
         else
            %% Forward the message using the routing table
            {@FingerTable route(msg:Event src:Event.src to:Target)}
         end
      end

      proc {ClosestPreceedingRoute Event}
         Msg = Event.msg
	 Target = Event.to
         in
         if {BelongsTo Target @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible, This message is for me
            {Self Msg}
         else
            %% Forward the message using the routing table
            {@FingerTable Event}
         end
      end

      proc {Update CandidatePbeer}
        if CandidatePbeer.id \= @SelfRef.id andthen {Not {PbeerList.isIn CandidatePbeer @Crashed}} then
            if {BelongsTo CandidatePbeer.id @SelfRef.id @Succ.id} andthen 
               CandidatePbeer.id\=@Succ.id then
                %OldSucc = @Succ.id
                %in
                {@ComLayer setPriority(@Succ priority:false)}
	        Succ := CandidatePbeer
                {Monitor CandidatePbeer true}
                {@FingerTable monitor(CandidatePbeer)}
                {Zend @Succ fix(src:@SelfRef)}
 		{@Listener clientEvents(msg:succChanged(@Succ.id))}
            elseif {BelongsTo CandidatePbeer.id @Pred.id @SelfRef.id} andthen
                   CandidatePbeer.id\=@Pred.id then
                OldPred = @Pred
                in
                 PredList := {AddToList CandidatePbeer @PredList}
                 Pred := CandidatePbeer 
                 {Monitor CandidatePbeer true}
                 {@FingerTable monitor(CandidatePbeer)}
		 %% Tell data management to migrate data in range ]OldPred, Pred]
            	 {@Listener newPred(old:OldPred new:@Pred tag:data)}
	         {@ComLayer setPriority(OldPred priority:false)}
                 {@Listener clientEvents(msg:predChanged(@Pred.id))}
                 if IS_VISUAL_DEBUG == 1 then
                      {@Logger event(@SelfRef.id predChanged(@Pred.id OldPred.id) color:darkblue)}
                      {@Logger event(@SelfRef.id onRing(true) color:darkblue)} %TODO: Check
                 end
            end
        end 
      end

      proc {WatchPeers Peers}
         {RingList.forAll Peers
            proc {$ Peer}
               {@ComLayer monitor(Peer priority:false)}
            end}
      end

      proc {Zend Target Msg}
         {@ComLayer sendTo(Target Msg log:rlxring)}
      end

      %%--- Events ---

      proc {Alive alive(Pbeer)}
         Crashed  := {PbeerList.remove Pbeer @Crashed}
         {@FingerTable monitor(Pbeer)}
         {@Listener clientEvents(msg:falsesuspicion(Pbeer.id))}
         local
		CurKBItem = {Dictionary.condGet KnowledgeBase Pbeer.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Pbeer.id):={Record.adjoinAt Pbeer timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Pbeer.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         if {BelongsTo Pbeer.id @Pred.id @SelfRef.id-1} then
            OldPred = @Pred
            in
            Phase:=liquid
            PredList := {AddToList Pbeer @PredList}
            Pred := Pbeer %% Monitoring Pbeer and it's on predList
            %% Tell data management to migrate data in range ]OldPred, Pred]
            {@Listener newPred(old:OldPred new:@Pred tag:data)}
	    {@ComLayer setPriority(@Pred priority:true)}
	    {@ComLayer setPriority(OldPred priority:false)}
            {@Listener clientEvents(msg:predChanged(@Pred.id))}
         elseif {BelongsTo Pbeer.id @SelfRef.id @Succ.id-1} then
	    Phase:=liquid	 
            {@ComLayer setPriority(@Succ priority:false)} 
            Succ := Pbeer
            {Zend @Succ fix(src:@SelfRef)}
            {@ComLayer setPriority(@Succ priority:true)}
            {@Listener clientEvents(msg:succChanged(@Succ.id))}
         else
             skip
        end 
      end

      proc {Any Event}
         {@Listener Event}
      end

      proc {BadRingRef Event}
         badRingRef = Event
      in
         {System.show 'BAD ring reference. I cannot join'}
         skip %% TODO: trigger some error message
      end

      proc {Crash crash(Pbeer)}
         Crashed  := {PbeerList.add Pbeer @Crashed}
         SuccList := {RingList.remove Pbeer @SuccList}
         PredList := {RingList.remove Pbeer @PredList}
	 {@Listener nodeCrash(node:Pbeer tag:trapp)}
         {@FingerTable removeFinger(Pbeer)}
         {@Listener clientEvents(msg:suspect(Pbeer.id))}
  	 {@ComLayer setPriority(Pbeer priority:false)}
         if Pbeer.id == @Succ.id then
            Phase:=liquid
            Succ := {RingList.getFirst @SuccList @SelfRef}
            {Monitor @Succ true}
            {Zend @Succ fix(src:@SelfRef)}
            
	    {@Listener clientEvents(msg:succChanged(@Succ.id))}
         end
         if Pbeer.id == @Pred.id then
            Phase:=liquid
            if @PredList \= nil then
               Pred := {RingList.getLast @PredList @SelfRef}
               in
               if @Pred.id\=@SelfRef.id then
                  {Monitor @Pred true}
                  %% Tell data management to migrate data in range ]Pred, OldPred]
                  {@Listener newResponsibilities(old:Pbeer.id new:@Pred.id tag:trapp)}    
                  {@Listener clientEvents(msg:predChanged(@Pred.id))} 
               end          
            end
         end
      end

      proc {ComputePhase computePhase}
         OldPhase = @CurrentPhase
         NewPhase
         in
         {GetPhase getPhase(NewPhase)}
         if OldPhase \= NewPhase then
		{@Listener clientEvents(msg:phasetransition(old:OldPhase new:NewPhase))}
         end
      end

      %% DSend
      %% Send directly to a port with the correct format
      proc {DSend Event}
         Msg = Event.1
         To = Event.to
      in
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase To.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(To.id):={Record.adjoinAt To timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(To.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         if {HasFeature Event log} then   
            {@ComLayer sendTo(To Msg log:Event.log)}
         else
            {@ComLayer sendTo(To Msg)}
         end
      end

      %%% Midnattsol
      %% Fix means 'Self is Src's new succ' and 'Src wants to be Self's pred'
      %% Src is accepted as predecessor if:
      %% 1 - the current predecessor is dead
      %% 2 - Src is in (pred, self]
      %% Otherwise is a better predecessor of pred.
      proc {Fix fix(src:Src)}
	 OldPred = @Pred
	 in
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Src.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         %% Src thinks I'm its successor so I add it to the predList
         PredList := {AddToList Src @PredList}
         {Monitor Src false}
         if {PbeerList.isIn @Pred @Crashed} then
            Phase:=solid
            Pred := Src %% Monitoring Src already and it's on predList
            {Zend Src fixOk(src:@SelfRef succList:@SuccList)}
            {@ComLayer setPriority(@Pred priority:true)}
            
            %% Tell data management to migrate data in range ]Pred, OldPred]
            {@Listener newResponsibilities(old:OldPred.id new:@Pred.id tag:trapp)}
	    {@ComLayer setPriority(OldPred priority:false)}
            {@Listener clientEvents(msg:predChanged(@Pred.id))}
         elseif {BelongsTo Src.id @Pred.id @SelfRef.id-1} then
            Phase:=solid
            Pred := Src %% Monitoring Src already and it's on predList
            {Zend Src fixOk(src:@SelfRef succList:@SuccList)}
            {@ComLayer setPriority(@Pred priority:true)}
            %% Tell data management to migrate data in range ]OldPred, Pred]
            {@Listener newPred(old:OldPred new:@Pred tag:data)}
	    {@ComLayer setPriority(OldPred priority:false)}
            {@Listener clientEvents(msg:predChanged(@Pred.id))}
         else
            %{System.show 'GGGGGGGGGGRRRRRRRRRRRAAAAAAAAAA'#@SelfRef.id}
            %% Just keep it in a branch
            %{RlxRoute predFound(pred:Src last:true) Src.id Self}
            Phase:=liquid
            skip
         end
      end

      proc {FixOk fixOk(src:Src succList:SrcSuccList)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Src.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         {RingList.forAll SrcSuccList proc {$ Pbeer}
					  CurKBItem = {Dictionary.condGet KnowledgeBase Pbeer.id nil}
					  in
					  if CurKBItem == nil then
         					KnowledgeBase.(Pbeer.id):={Record.adjoinAt Pbeer timestamp {BootTime.getReferenceTime}}
					  else
						KnowledgeBase.(Pbeer.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
					  end
	 			       end}
         Phase:=solid                                
         SuccList := {UpdateList @SuccList Src SrcSuccList}
         {Zend @Pred updSuccList(src:@SelfRef
                                 succList:@SuccList
                                 counter:SLSize)}
      end

      proc {GetComLayer getComLayer(Res)}
         Res = @ComLayer
      end

      proc {GetFullRef getFullRef(FullRef)}
         FullRef = ref(pbeer:@SelfRef ring:@Ring)
      end

      proc {GetId getId(Res)}
         Res = @SelfRef.id
      end

      proc {GetMaxKey getMaxKey(Res)}
         Res = MaxKey
      end

      proc {GetPred getPred(Peer)}
         Peer = @Pred
      end

      proc {GetRange getRange(Res)}
      	Res = (@Pred.id+1 mod MaxKey)#@SelfRef.id
      end

      proc {GetRef getRef(Res)}
      	Res = @SelfRef
      end

      proc {GetRingRef getRingRef(RingRef)}
         RingRef = @Ring
      end

      proc {GetSucc getSucc(Peer)}
         Peer = @Succ
      end

      proc {GetKnowledge getKnowledge(KnownPeers)}
         KnownPeers = {Dictionary.items KnowledgeBase}
      end

      proc {GetIdKnowledge getIdKnowledge(KnownPeerIds)}
         KnownPeerIds = {Dictionary.keys KnowledgeBase}
      end

      proc {GetBranchSz getBranchSz(Res)}
          Res = {List.length @PredList}
      end

      proc {GetPhase getPhase(Res)}
	if @Succ.id==@SelfRef.id andthen (@Pred.id == @SelfRef.id orelse {PbeerList.isIn @Pred @Crashed}) then
		Phase := gas
	else
                SzPredList = {NewCell 0}
	        FTStatus FTConvergence
	        HasPredConn = {NewCell true}
		in
		SzPredList := {List.length {RingList.remove @SelfRef @PredList}}
	        {@FingerTable getFingersStatus(FTStatus FTConvergence)}
	        if @SzPredList>1 then
			for _#P in @PredList do
				if P.id \= @SelfRef.id andthen P.id \= @Pred.id then
					CurKBItem = {Dictionary.condGet KnowledgeBase P.id nil}
					in
					if CurKBItem \= nil andthen {HasFeature CurKBItem predTS} then
						ElapsedTime = {BootTime.getReferenceTime}-CurKBItem.predTS
						in
						if ElapsedTime>Constants.predCandidateValidityFactor*Constants.pSPeriod then
							SzPredList := @SzPredList-1
                                        	end
					else
						SzPredList := @SzPredList-1
                                	end
                  		end
			end
		end
                local
			CurKBItem = {Dictionary.condGet KnowledgeBase @Pred.id nil}
		        in
			if CurKBItem \= nil andthen {HasFeature CurKBItem predTS} then
				ElapsedTime = {BootTime.getReferenceTime}-CurKBItem.predTS
				in
				if ElapsedTime>Constants.predConnValidityFactor*Constants.pSPeriod then
					HasPredConn:=false
				end
			else
				HasPredConn:=false
			end
		end
			
		if {Not @HasPredConn} orelse @SzPredList > 1 orelse @Succ.id==@SelfRef.id orelse @Phase==liquid orelse ({Not FTConvergence} andthen FTStatus>25.0) then
                        if {Not @HasPredConn} orelse @Succ.id==@SelfRef.id orelse @Pred.id == @SelfRef.id then
				Phase := liquid3
			else
				if @SzPredList > 1 then
					Phase := liquid1
				else
					Phase := liquid2
				end
			end
		else
			Phase := solid
		end
	end
        CurrentPhase := @Phase
        Res = @Phase
      end

      proc {Hint hint(succ:NewSucc)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase NewSucc.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(NewSucc.id):={Record.adjoinAt NewSucc timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(NewSucc.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         if {BelongsTo NewSucc.id @SelfRef.id @Succ.id-1} then
            {Zend @Succ predNoMore(@SelfRef)}
	    {@ComLayer setPriority(@Succ priority:false)}
            Succ := NewSucc
            {Monitor NewSucc true}
            {@FingerTable monitor(NewSucc)}
            {Zend @Succ fix(src:@SelfRef)}
            {@Listener clientEvents(msg:succChanged(@Succ.id))}
         end
      end

      proc {IdInUse idInUse(Id)}
         %%TODO. Get a new id and try to join again
         if @SelfRef.id == Id then
            {System.show 'I cannot join because my Id is already in use'}
	    {@Listener clientEvents(msg:idInUse(Id))}
         else
            {System.showInfo '#'("My id " @SelfRef.id
                                 " is considered to be in use as " Id)}
         end
      end

      proc {Init Event}
         skip
      end

     proc {SignalDestroy Event}
         if @PSFlag then {SelfStabilizer signalDestroy} end
         if @MergerFlag then {SelfMerger signalDestroy} end
         {Suicide}
     end

      proc {Join Event}
         Src = Event.src
 	 SrcRing = Event.ring 
         %% Event join might come with flag last = true guessing to reach the
         %% responsible. If I am not the responsible, message has to be 
         %% backwarded to the branch. 
      in
         if @Ring.name\=SrcRing.name andthen @Ring.id \= SrcRing.id then
            {Zend Src badRingRef}
         elseif @SelfRef.id == Src.id then
            {Zend Src idInUse(Src.id)}
         elseif {Not {PbeerList.isIn @Succ @Crashed}} then
            if {BelongsTo Src.id @Pred.id @SelfRef.id} then
               OldPred = @Pred
            in
               {Zend Src joinOk(pred:OldPred
                                succ:@SelfRef
                                succList:@SuccList)}
               Pred := Src
               {Monitor Src true} 
               /*if @PredList \= nil then		%% Code to prune branch
                  CloserPeerOnBranch = {RingList.getLast @PredList nil}
                  in
                  if CloserPeerOnBranch\=nil then
                    {Zend CloserPeerOnBranch hint(succ:Src)}
                  end
               end*/     
               
               PredList := {AddToList @Pred @PredList}
               %% Tell data management to migrate data in range ]OldPred, Pred]
               {@Listener newPred(old:OldPred new:@Pred tag:data)}
               {@ComLayer setPriority(OldPred priority:false)}
	       KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
               {@Listener clientEvents(msg:predChanged(@Pred.id))}
            else
               %NOT FOR ME - going to route
	       KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
               {RlxRoute Event Src.id}
            end
         else
            {Zend Src joinLater(@SelfRef)}
         end
      end

      proc {PredNoMore predNoMore(OldPred)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase OldPred.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(OldPred.id):={Record.adjoinAt OldPred timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(OldPred.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         PredList := {RingList.remove OldPred @PredList}
         %% TODO: Add treatment of hint message here
      end

      proc {JoinLater joinLater(NewSucc)}
         {Timer startTrigger(JOIN_WAIT startJoin(succ:NewSucc ring:@WishedRing) Self)}
      end

      proc {JoinOk joinOk(pred:NewPred succ:NewSucc succList:NewSuccList)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase NewPred.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(NewPred.id):={Record.adjoinAt NewPred timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(NewPred.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase NewSucc.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(NewSucc.id):={Record.adjoinAt NewSucc timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(NewSucc.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         
         {RingList.forAll NewSuccList proc {$ Pbeer}
						CurKBItem = {Dictionary.condGet KnowledgeBase Pbeer.id nil}
						in
						if CurKBItem == nil then
         						KnowledgeBase.(Pbeer.id):={Record.adjoinAt Pbeer timestamp {BootTime.getReferenceTime}}
						else
							KnowledgeBase.(Pbeer.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
						end
	 			      end}
         if {BelongsTo NewSucc.id @SelfRef.id @Succ.id} then
            Succ := NewSucc
            SuccList := {UpdateList @SuccList NewSucc NewSuccList}
            Ring := @WishedRing
            WishedRing := none
            {Monitor NewSucc true} 
            {@FingerTable monitor(NewSucc)}
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
            if {Value.isFree FirstAck} then
            	FirstAck = unit
            	{@Listener clientEvents(msg:joinack)}
            end
            {@Listener clientEvents(msg:succChanged(@Succ.id))}
	    Phase:=liquid
         end
         if {BelongsTo NewPred.id @Pred.id @SelfRef.id} then
            {Zend NewPred newSucc(newSucc:@SelfRef succList:@SuccList)}
            Pred := NewPred
            PredList := {AddToList NewPred @PredList}
            %% set a failure detector on the predecessor
            {Monitor NewPred true} 
            {@FingerTable monitor(NewPred)}
            {@Listener clientEvents(msg:predChanged(@Pred.id))}
         end
      end

      proc {Lookup lookup(key:Key res:Res)}
         HKey
      in
         HKey = {Utils.hash Key MaxKey}
         {LookupHash lookupHash(hkey:HKey res:Res)}
      end

      proc {LookupHash lookupHash(hkey:HKey res:Res)}
         {Route route(msg:lookupRequest(res:Res) src:@SelfRef to:HKey)}
      end

      proc {LookupRequest lookupRequest(res:Res)}
         %% TODO: mmm... can we trust distributed variable binding?
         Res = @SelfRef
      end

     proc {MakeAQueueInsert Event}
	if @MergerFlag then
        	{SelfMerger Event}
	end
     end

     proc {IntroduceANode introduce(P)}
	local
		CurKBItem = {Dictionary.condGet KnowledgeBase P.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(P.id):={Record.adjoinAt P timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(P.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
        {MakeAQueueInsert makeAQueueInsert(P)}
     end

     proc {MLookup Event}
         Target = Event.id
         F = Event.fanout
         in
         if F > 1 then
            skip %% I'll do something
         end

	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Target.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Target.id):={Record.adjoinAt Target timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Target.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         if Target.id \= @SelfRef.id andthen Target.id \= @Succ.id then
            if {BelongsTo Target.id @SelfRef.id @Succ.id} then
               {Zend Target retrievePred(src:@SelfRef psucc:@Succ tag:ps)}
            else
               {ClosestPreceedingRoute closestPreceedingRoute(msg:Event src:@SelfRef to:Target.id)}
            end
         end
         {Update Target}
      end

      proc {NewSucc newSucc(newSucc:NewSucc succList:NewSuccList)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase NewSucc.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(NewSucc.id):={Record.adjoinAt NewSucc timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(NewSucc.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         {RingList.forAll NewSuccList proc {$ Pbeer}
					CurKBItem = {Dictionary.condGet KnowledgeBase Pbeer.id nil}
					in
					if CurKBItem == nil then
         					KnowledgeBase.(Pbeer.id):={Record.adjoinAt Pbeer timestamp {BootTime.getReferenceTime}}
					else
						KnowledgeBase.(Pbeer.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
					end
				      end}
                                          
         if {Not {Value.isFree FirstAck}} andthen {BelongsTo NewSucc.id @SelfRef.id @Succ.id} then
            SuccList := {UpdateList @SuccList NewSucc NewSuccList}
            {Zend @Succ predNoMore(@SelfRef)}
            {Zend @Pred updSuccList(src:@SelfRef
                                    succList:@SuccList
                                    counter:SLSize)}
	    {@ComLayer setPriority(@Succ priority:false)}
            Succ := NewSucc
            {Monitor NewSucc true}
            {@FingerTable monitor(NewSucc)}
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
	    {@Listener clientEvents(msg:succChanged(@Succ.id))}
         end
      end

      proc {RetrievePred retrievePred(src:Src psucc:PSucc tag:ps)}
 	 {Zend Src retrievePredRes(src:@SelfRef
                                  succp:@Pred
                                  succList:@SuccList tag:ps)}
         if PSucc.id \= @SelfRef.id then
               {MakeAQueueInsert makeAQueueInsert(PSucc)}
         end
         {Update Src}
         local
		CurKBItem = {Dictionary.condGet KnowledgeBase Src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Src.id):={Record.adjoinAt {Record.adjoinAt Src timestamp {BootTime.getReferenceTime}} predTS {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Src.id):={Record.adjoinAt {Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}} predTS {BootTime.getReferenceTime}}
		end
	 end
         local
		CurKBItem = {Dictionary.condGet KnowledgeBase PSucc.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(PSucc.id):={Record.adjoinAt PSucc timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(PSucc.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
        
         if Src.id ==  @Pred.id andthen PSucc.id==@SelfRef.id then
		Phase:=solid
 	 end
      end

      proc {RetrievePredRes retrievePredRes(src:Src succp:SuccP succList:SuccSL tag:ps)}
         {Update SuccP}
         {UpdSuccList updSuccList(src:Src succList:SuccSL counter:SLSize)}
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Src.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         local
		CurKBItem = {Dictionary.condGet KnowledgeBase SuccP.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(SuccP.id):={Record.adjoinAt SuccP timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(SuccP.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         
         if SuccP.id \= @SelfRef.id andthen SuccP.id \= @Succ.id then
                Phase:=liquid
		{MakeAQueueInsert makeAQueueInsert(SuccP)}
	end

        if SuccP.id==@SelfRef.id andthen Src.id==@Succ.id then      
		Phase:=solid		
         end
      end

      proc {Route Event}
         Msg = Event.msg
	 Target = Event.to
      in
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Event.src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Event.src.id):={Record.adjoinAt Event.src timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Event.src.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         if {BelongsTo Target @Pred.id @SelfRef.id} then
            %% This message is for me
            if {HasFeature Event last} andthen Event.last then
               if {HasFeature Event branch} andthen Event.branch then
                  Phase:=liquid
               else
                  Phase:=solid
               end
            end
            {Self Msg}
         elseif {HasFeature Event last} andthen Event.last then
            %% I am supposed to be the responsible, but I have a branch
            %% or somebody was missed (non-transitive connections)
            {Backward Event Target}
         elseif {BelongsTo Target @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible => set last = true
            %{System.show @SelfRef.id#' I missed one week? '#Event}
            {Zend @Succ {Record.adjoinAt Event last true}}
            %{Blabla @SelfRef.id#" forwards join of "#Sender.id#" to "
            %         #@(Self.succ).id}
         else
            %% Forward the message using the routing table
            {@FingerTable Event}
            %{Blabla @SelfRef.id#" forwards join of "#Src.id}
         end
      end

      proc {SetFingerTable setFingerTable(NewFingerTable)}
         FingerTable := NewFingerTable
      end

      proc {SetLogger Event}
	setLogger(NewLogger) = Event
      in
	Logger := NewLogger
         %{@ComLayer Event}
	 {@ComLayer setLogger(NewLogger)}
      end

      proc {SetPhaseNotify setPhaseNotify}
        SelfPhaseComputer := {PeriodicPhaseCalculator.new}
        {@SelfPhaseComputer setComLayer(@ComLayer)}
        {@SelfPhaseComputer setListener(Self)}
      end

      proc {Stabilize stabilize}
         FingerList
         fun {IsRelatedtoMe Peer}
	    if @SelfRef.id==Peer.id andthen @Succ.id==Peer.id andthen @Pred.id==Peer.id then
               true
            else
	       {RingList.isIn Peer @SuccList} orelse {RingList.isIn Peer FingerList}	
            end
         end
         in
         {@FingerTable getFingers(FingerList)}
         if @Succ.id==@SelfRef.id andthen @Pred.id == @SelfRef.id then
            FailedAttempt:=@FailedAttempt+1
            if {Value.isFree FirstAck} andthen @FailedAttempt>@CurAck*ATTEMPT_TO_JOIN then
		{@Listener clientEvents(msg:isolation)}
                CurAck := @CurAck+1
                FailedAttempt:=0
            else
                if @FailedAttempt>@CurAck*ATTEMPT_TO_JOIN then
                   CurAck := @CurAck+1
                   FailedAttempt:=0
                   {@Listener clientEvents(msg:joinedbutisolated)}
                elseif @FailedAttempt mod 10 == 0 then           
		   if {Not {RingList.isEmpty @SuccList}} then
		        {RingList.forOne @SuccList proc {$ Pbeer}
                                                      {MakeAQueueInsert makeAQueueInsert(Pbeer)}
                                                   end @SelfRef}
                   elseif {Not {RingList.isEmpty FingerList}} then
		         {RingList.forOne FingerList proc {$ Pbeer}
                                                      {MakeAQueueInsert makeAQueueInsert(Pbeer)} 
                                                    end @SelfRef}
                   end
                end
            end
         else
            FailedAttempt:=0
            if @Succ.id==@SelfRef.id then
                 {RetrievePred retrievePred(src:@SelfRef psucc:@Succ tag:ps)}
            else
                 {Zend @Succ retrievePred(src:@SelfRef psucc:@Succ tag:ps)}
            end
         end
         if @KBFlag then
           local 
           KnowledgeList = {Dictionary.items KnowledgeBase}
           in
           {List.forAll KnowledgeList proc {$ P}
                                             if {Not {PbeerList.isIn P @Crashed}} andthen 
						{Not {IsRelatedtoMe P}} andthen {Random.urand}>0.9 then
                                                {MakeAQueueInsert makeAQueueInsert(P)}
                                             end
                                      end}
           end
 	 end
      end

      proc {StartJoin startJoin(succ:NewSucc ring:RingRef)}
         %if {Value.isFree FirstAck} then
            WishedRing := RingRef
            {Zend NewSucc join(src:@SelfRef ring:RingRef)}
         %end
         %KnowledgeBase.(NewSucc.id):=NewSucc
      end

      proc {StartPS startPS}
         PSFlag := true
         SelfStabilizer = {PeriodicStabilizer.new}
         {SelfStabilizer setComLayer(@ComLayer)}
         {SelfStabilizer setListener(Self)}
      end

      proc {StartMerger startMerger}
         MergerFlag := true
         SelfMerger = {Merger.new}
         {SelfMerger setComLayer(@ComLayer)}
         {SelfMerger setListener(Self)}
      end

      proc {StartKB startKB}
         KBFlag:=true
      end

      proc {StopPS stopPS}
         PSFlag := false
         {SelfStabilizer signalDestroy}
      end

      proc {StopMerger stopMerger}
         MergerFlag := false
         {SelfMerger signalDestroy}
      end

      proc {StopKB stopKB}
         KBFlag:=false
      end

      proc {StopPhaseNotify Event}
	if @SelfPhaseComputer \= nil then
           {@SelfPhaseComputer stopPhaseTracker}
           SelfPhaseComputer := nil
        end
      end

      proc {UpdSuccList Event}
         updSuccList(src:Src succList:NewSuccList counter:Counter) = Event
      in
	 local
		CurKBItem = {Dictionary.condGet KnowledgeBase Src.id nil}
		in
		if CurKBItem == nil then
         		KnowledgeBase.(Src.id):={Record.adjoinAt Src timestamp {BootTime.getReferenceTime}}
		else
			KnowledgeBase.(Src.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
		end
	 end
         {RingList.forAll NewSuccList proc {$ Pbeer}
					CurKBItem = {Dictionary.condGet KnowledgeBase Pbeer.id nil}
					in
					if CurKBItem == nil then
         					KnowledgeBase.(Pbeer.id):={Record.adjoinAt Pbeer timestamp {BootTime.getReferenceTime}}
					else
						KnowledgeBase.(Pbeer.id):={Record.adjoinAt CurKBItem timestamp {BootTime.getReferenceTime}}
					end
	 			      end}
                                          
         if @Succ.id == Src.id then
            SuccList := {UpdateList @SuccList Src NewSuccList}
            if Counter > 0 then
               {Zend @Pred updSuccList(src:@SelfRef
                                       succList:@SuccList
                                       counter:Counter - 1)}
            end
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
         end
      end

      ToFingerTable = {Utils.delegatesTo FingerTable}

      Events = events(
                  alive:         Alive
                  any:           Any
                  crash:         Crash
                  badRingRef:    BadRingRef
                  computePhase:  ComputePhase
                  dsend:         DSend
                  fix:           Fix
                  fixOk:         FixOk
                  getComLayer:   GetComLayer
                  getFullRef:    GetFullRef
                  getId:         GetId
                  getMaxKey:     GetMaxKey
                  getPred:       GetPred
                  getRange:      GetRange
                  getRef:        GetRef
                  getRingRef:    GetRingRef
                  getSucc:       GetSucc
                  getKnowledge:  GetKnowledge
                  getIdKnowledge:  GetIdKnowledge
                  getBranchSz:   GetBranchSz
 		  getPhase:	 GetPhase
                  hint:          Hint
                  idInUse:       IdInUse
                  init:          Init
                  introduce:     IntroduceANode
                  signalDestroy: SignalDestroy
                  join:          Join
                  joinLater:     JoinLater
                  joinOk:        JoinOk
                  lookup:        Lookup
                  lookupHash:    LookupHash
                  lookupRequest: LookupRequest
                  mlookup:       MLookup
                  makeAQueueInsert: MakeAQueueInsert
                  needFinger:    ToFingerTable
                  newFinger:     ToFingerTable
                  newSucc:       NewSucc
                  predNoMore:    PredNoMore
                  route:         Route
                  closestPreceedingRoute: ClosestPreceedingRoute
                  refreshFingers:ToFingerTable
                  retrievePred:  RetrievePred
                  retrievePredRes:RetrievePredRes 
                  setFingerTable:SetFingerTable
                  setLogger:     SetLogger
                  setPhaseNotify: SetPhaseNotify
                  stabilize:     Stabilize
                  startJoin:     StartJoin
		  startPS:       StartPS
                  startMerger:   StartMerger
                  startKB:       StartKB
                  stopPS:       StopPS
                  stopMerger:   StopMerger
                  stopKB:       StopKB
                  stopPhaseNotify: StopPhaseNotify
                  updSuccList:   UpdSuccList
		  %useSuccBackUp: UseSuccBackUp
                  )

   in %% --- New starts ---
      %% Creating the component and collaborators
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Listener = FullComponent.listener
         Suicide  = FullComponent.killer
      end
      Timer = {TimerMaker.new}
      ComLayer = {NewCell {Network.new}}
      {@ComLayer setListener(Self)}

      KnowledgeBase = {Dictionary.new}

      Logger      = {NewCell Component.dummy}	%(R)

      Args        = {Utils.addDefaults CallArgs
                                       def(firstAck:_
                                           maxKey:MAX_KEY
                                           slsize:SL_SIZE
                                           ps:false
                                           merger:false
                                           kb:false)}
      FirstAck    = Args.firstAck
      MaxKey      = Args.maxKey
      SLSize      = Args.slsize
      PSFlag      = {NewCell Args.ps}
      MergerFlag  = {NewCell Args.merger}
      KBFlag      = {NewCell Args.kb}

      %% Peer State
      if {HasFeature Args id} then
         SelfRef = {NewCell pbeer(id:Args.id)}
      else
         SelfRef = {NewCell pbeer(id:{KeyRanges.getRandomKey MaxKey})}
      end
      SelfRef := {Record.adjoinAt @SelfRef port {@ComLayer getPort($)}}
      {@ComLayer setId(@SelfRef.id)}
      
      if {HasFeature Args fdParams} then
         {@ComLayer setFDParams(Args.fdParams)}
      end

      if @PSFlag then
         {StartPS startPS}
      end


      Pred        = {NewCell @SelfRef}
      Succ        = {NewCell @SelfRef}
      PredList    = {NewCell {RingList.new}}
      SuccList    = {NewCell {RingList.new}} 
      Crashed     = {NewCell {PbeerList.new}}
      Ring        = {NewCell ring(name:lucifer id:{Name.new})}
      WishedRing  = {NewCell start}
      FingerTable = {NewCell BasicForward}
      FailedAttempt = {NewCell 0}
      CurAck      = {NewCell 1}
      Phase	  = {NewCell gas}
      CurrentPhase = {NewCell gas}
      SelfPhaseComputer = {NewCell nil}

      %% For ReCircle
      if @MergerFlag then
         {StartMerger startMerger}
      end 
     
      %% Return the component
      Self
   end
end
