/*-------------------------------------------------------------------------
 *
 * PaxosEager-TM.oz
 *
 *    Transaction Manager for the Eager Paxos Consensus Algorithm    
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 15 $ $Author: ruma $
 *
 *    $Date: 2016-04-28 11:38:29 +0200 (Thurs, 28 April 2016) $
 *
 * NOTES
 *
 *    Implementation of Leader (TM) and replicated transaction managers (rTMs)
 *    for the Eager Paxos Consensus algorithm protocol. The main difference with
 *    Paxos is that Eager-Paxos acquires locks of relevant keys to proceed 
 *    with a transaction. The locking-phase is followed by the commit-phase.
 *    
 *-------------------------------------------------------------------------
 */


functor
import
   Property
   Component      at '../../corecomp/Component.ozf'
   Constants      at '../../commons/Constants.ozf'
   Timer          at '../../timer/Timer.ozf'
   Utils          at '../../utils/Misc.ozf'
   BootTime       at 'x-oz://boot/Time'
   Random         at '../../utils/Random.ozf'
   %System
export
   New
define
   BAD_SECRET  = Constants.badSecret
   NO_SECRET   = Constants.public
   NO_VALUE    = Constants.noValue
   Debug       = Utils.blabla
   
   fun {New Args}
      Self
      Suicide
      Listener
      MsgLayer
      Replica
      TheTimer

      LockKey        % A unique key for a transaction, to avoid bug of Name variables
      Client         % Client port to communicate final decision
      LockHolderPbeer % Pbeer who is requesting for a lock, needed to notify change of lock
      Id             % Id of the transaction manager object
      Tid            % Id of the transaction
      RepFactor      % Replication Factor
      NodeRef        % Node Reference
      FinalDecision  % Decision taken after collecting votes
      Leader         % Transaction Leader
      LocalStore     % Stores involve items with their new values and operation
      Votes          % To collect votes from Transaction Participants
      VotingPeriod   % Time to vote for TPs
      VotingPolls    % Register time for voting
      VotesAcks      % Collect decided items from rTMs
      Role           % Role of the TM: leader or rtm
      RTMs           % Set of replicated transaction managers rTMs
      Done           % Flag to know when we are done
      MaxKey         % To use the hash function

      TPs            % Direct reference to transaction participants
      VotedItems     % Collect items once enough votes are received 

      PeriodToCommit % Max time to commit after acquiring the lock

      TMRank	     % Needed for Leader Election in case of failure of current leader
      RTMCount       % Needed to assign rank for RTM
      CurrentRound   % Needed to elect new leader in case of failure of current leader
      LEPeriod       % Time for new leader to respond
      Suspected      % TMs suspected by this TM	

      CurrentLeaderLastResponse % Being a RTM When did I receive last response from my leader?
      

      %% --- Util functions -------------------------------------------------
      fun lazy {GetRemote Key}
         Item
         RemoteItem
         MostItems
         fun {GetNewest L Newest}
            case L
            of H|T then
               NewReaderList = {NewCell Newest.readers}
               in
               for P in H.readers do
                   if {Not {IsInList @NewReaderList P}} then
                         NewReaderList := P|@NewReaderList
                   end
               end
               if H.version > Newest.version then
                  {GetNewest T {Record.adjoinAt H readers @NewReaderList}}
               else
                  {GetNewest T {Record.adjoinAt Newest readers @NewReaderList}}
               end
            [] nil then
               Newest
            [] 'NOT_FOUND' then
               %% TODO: Check this case. There should be always a list
               Newest
            end
         end
         fun {IsInList L Peer}
            case L
             of H|T then
                if H.id == Peer.id then
                    true
                else
                    {IsInList T Peer}
                end
             [] nil then
                false
            end
         end
         in
         MostItems = {@Replica getAll(Key $ trapp)}
         if {IsList MostItems} then skip end
         %{System.show 'For Key '#Key#' found majority '#MostItems}
         RemoteItem = {GetNewest MostItems item(key:     Key
                                    		secret:  NO_SECRET
                                    		value:   'NOT_FOUND'
                                    		version: 0
                                    		readers: nil)}         
         Item = {Record.adjoinAt RemoteItem op read}
         %{System.show 'For Key '#Key#' decided majority value '#Item}
         Item
      end

      fun {GetItem Key}
         KeyItem = {Dictionary.condGet LocalStore Key {GetRemote Key}}
	in
	if{Not {Dictionary.member LocalStore Key}} then
		LocalStore.Key := KeyItem
        end
	KeyItem
      end

       %% AnyMajority uses a timer to wait for all TPs instead of claiming
      %% majority as soon as it is reached.
      fun {AnyMajority Key}
         fun {CountBrewed L Acc}
            case L
            of Vote|MoreVotes then
               if Vote.vote == brewed then
                  {CountBrewed MoreVotes Acc+1}
               else
                  {CountBrewed MoreVotes Acc}
               end
            [] nil then
               Acc
            end
         end
         TheVotes
      in
         TheVotes = Votes.Key
         if VotingPolls.Key == open andthen {Length TheVotes} < @RepFactor then
            none
         else
            VotingPolls.Key := close
            if {CountBrewed TheVotes 0} > @RepFactor div 2 then
               brewed
            else
               denied
            end
         end
      end

      fun {EnoughRTMacks Keys}
         case Keys
         of K|MoreKeys then
            if {Length VotesAcks.K} >= @RepFactor div 2 then
               {EnoughRTMacks MoreKeys}
            else
               false
            end
         [] nil then
            true
         end
      end

      fun {GotAllBrewed}
         fun {Loop L}
            case L
            of Vote|MoreVotes then
               if Vote.consensus == brewed then
                  {Loop MoreVotes}
               else
                  false
               end
            [] nil then
               true
            end
         end
      in
         {Loop @VotedItems}
      end

      proc {SpreadDecision Decision}     
         %% Send to all TPs
         for I in {Dictionary.entries LocalStore} do 
            %{System.show 'Debug: Retrieved Entry '#I#' from localstore'}
            if Decision==granted then
		for Peer in I.2.readers do
                   if Peer.id \= I.2.client then
                     {@MsgLayer dsend(to:Peer notifyReaderLocking(key:I.2.key tag:trapp))}
                   end
              	end
            end
            local
 		TPList = {Dictionary.condGet TPs I.1 nil}
		in
            	for TP in TPList do
              		{@MsgLayer dsend(to:TP.ref final(decision:Decision
                                                	tid:     Tid
                                                	tpid:    TP.id
                                                	tag:     trapp
                                               	 	))}
	    	end
	    end
         end
         %% Send to all rTMs
         for TM in @RTMs do
            {@MsgLayer dsend(to:TM.ref setFinal(decision:Decision
                                                lockPeriod: @PeriodToCommit
                                                tid:     Tid
                                                tmid:    TM.id
                                                tag:     trapp))}
         end

         %{System.show 'Debug: Informing client decision '#Decision#' at leader with peer id '#@NodeRef.id}

         try
            {Port.send Client Decision#@NodeRef.id#Tid#Id#@PeriodToCommit}
         catch _ then
             %% TODO: improve exception handling
             skip
         end

         if Decision == abort then
            	{@Listener deleteTM(tid:Tid tmid:Id tag:trapp)}
         	{Suicide}
        else
           % start a timer to receive commit command, if timeout release lock and notify client
           {TheTimer startTrigger(@PeriodToCommit timeoutToCommit)}
	end
      end

     proc {CheckDecision}
         %{System.show 'Debug: Checking Decision at leader with peer id '#@NodeRef.id}
         if {Not @Done} andthen {Length @VotedItems} == {Length {Dictionary.keys Votes}} then
	   %{System.show 'Debug: Received vote for all items at leader with peer id '#@NodeRef.id#' waiting for RTMs ack'}
            %% Received All Votes, waiting for RTM acks
            %if {EnoughRTMacks {Dictionary.keys VotesAcks}} then
               FinalDecision = if {GotAllBrewed} then granted 
			       else abort end
               Done := true
               %{System.show 'Debug: Reached decision '#FinalDecision#' at leader with peer id '#@NodeRef.id}
               {SpreadDecision FinalDecision} 
           %end
         end
    end

    proc {CheckConsensus Key}
        Consensus
        in
        Consensus   = {AnyMajority Key}
        %{System.show 'Debug: current consensus result for key '#Key#' is '#Consensus#' at leader with peer id '#@NodeRef.id}
         if Consensus \= none then
            VotedItems := vote(key:Key consensus:Consensus) | @VotedItems
            if @Leader \= noref andthen @Leader.id == Id then
               {CheckDecision}
            else
               if @Leader \= noref then
               		{@MsgLayer dsend(to:@Leader.ref
                                	voteAck(key:    Key
                                        	vote:   Consensus
                                        	tid:    Tid
                                        	tmid:   @Leader.id
                                        	rtm:    @NodeRef
                                        	tag:    trapp))}
	        end
            end
         end
     end

     proc {AbortTransaction}
         for I in {Dictionary.items LocalStore} do
		        {@Replica bulk(to:{Utils.hash I.key @MaxKey}
                           			releaseLock(
                           		     	tid:     Tid
                                                lockkey: LockKey
                           		     	item:    I
                           		     	protocol:eagerpaxos
                           		     	tag:     trapp
                           		     	))} 
          end
          for TM in @RTMs do
              {@MsgLayer dsend(to:TM.ref notifyTermination(tid:     Tid
                                                	   tmid:    TM.id
                                                           tag:     trapp))}
          end
          {@Listener deleteTM(tid:Tid tmid:Id tag:trapp)}
          {Suicide}
      end


    %% === Events =========================================================

    proc {StartValidation}
         %% Notify all rTMs
         if @CurrentRound == 1 then
         	for RTM in @RTMs do
            		{@MsgLayer dsend(to:RTM.ref
                             rtms(@RTMs tid:Tid tmid:RTM.id tmrank:RTM.rank tag:trapp))}
		end
         end
         if @Leader \= noref then	%% Start voting proces only if Leader is okay
                %{System.show 'Debug: starting validation at leader with peer id '#@NodeRef.id}
		%% Initiate TPs per each item. Ask them to vote
        	for I in {Dictionary.items LocalStore} do
		        {@Replica bulk(to:{Utils.hash I.key @MaxKey}
                           			brew(leader:  @Leader
                           		     	rtms:    @RTMs
                           		     	tid:     Tid
                                                lockkey: LockKey
                           		     	item:    I
                           		     	protocol:eagerpaxos
                           		     	tag:     trapp
                           		     	))} 
            	        Votes.(I.key)  := nil
            		TPs.(I.key)    := nil
            	 	VotesAcks.(I.key) := nil
                end
         	%% Open VotingPolls and launch timers
         	for I in {Dictionary.items LocalStore} do
                     VotingPolls.(I.key) := open
                     {TheTimer startTrigger(@VotingPeriod timeoutPoll(I.key))}
                end
	end
      end

    proc {InitRTM initRTM(leader: TheLeader
                            tid:    TransId
                            lockkey: TheLockKey
                            client: TheClient
                            store:  StoreEntries
                            protocol:_
                            hkey:   _
                            lockholder: TheLockRequester
                            tag:    trapp
                            )}
         Tid         = TransId
         LockKey     = TheLockKey
         Leader      = {NewCell TheLeader}
         Client      = TheClient
         LockHolderPbeer = TheLockRequester
         for Key#I in StoreEntries do
            LocalStore.Key       := I
            Votes.(I.key)        := nil
            TPs.(I.key)          := nil
            VotesAcks.(I.key)    := nil
            VotingPolls.(I.key)  := open
         end
         %{System.show 'Debug: Initiated one RTM for leader'#TheLeader.ref.id#' at peer id '#@NodeRef.id}
         {@MsgLayer dsend(to:@Leader.ref registerRTM(rtm: tm(ref:@NodeRef id:Id rank:@TMRank)
                                                     tmid:@Leader.id
                                                     tid: Tid
                                                     tag: trapp))}
         CurrentLeaderLastResponse := {BootTime.getReferenceTime}
      end

     proc {RegisterRTM registerRTM(rtm:NewRTM tmid:_ tid:_ tag:trapp)}
         %{System.show 'Debug: Received one RTM init msg from peer id '#NewRTM.ref.id#' at leader with peer id '#@NodeRef.id}
         if {HasFeature NewRTM rank} andthen NewRTM.rank \= 0 andthen @CurrentRound>1 then
		RTMs := NewRTM|@RTMs
         else	
             	RTMs := {Record.adjoinAt NewRTM rank @RTMCount}|@RTMs
             	RTMCount := @RTMCount + 1
         end
         if {List.length @RTMs} == @RepFactor-1 andthen 
              @Leader\=noref andthen @Leader.id==Id then 
            	%% We are done with initialization. We start with validation
         	if {Not @Done} then
           		{StartValidation}
                else
                        {TheTimer startTrigger((2*@VotingPeriod) timeoutAcks)}
                end
	end
      end

      proc {NotifyTermination notifyTermination(tid:_ tmid:_ tag:trapp)}
         CurrentLeaderLastResponse := {BootTime.getReferenceTime}
         {@Listener deleteTM(tid:Tid tmid:Id tag:trapp)}
         {Suicide}
      end

      proc {SetFinal setFinal(decision:Decision lockPeriod:Period tid:_ tmid:_ tag:trapp)}
         CurrentLeaderLastResponse := {BootTime.getReferenceTime}
         if {Not @Done} then
            FinalDecision = Decision
            Done:=true
            if Decision==abort then
               {@Listener deleteTM(tid:Tid tmid:Id tag:trapp)}
               {Suicide}
            else
		PeriodToCommit := Period
            end
         end
      end

      proc {UpdateRTM updateRTM(tid:_/*Tid*/ tmid:_/*TMid*/ item:Item tag:trapp)}
         CurrentLeaderLastResponse := {BootTime.getReferenceTime}
         LocalStore.(Item.key) := Item
      end

      proc {SetRTMs rtms(TheRTMs tid:_ tmid:_ tmrank:Rank tag:trapp)}
         RTMs := TheRTMs
         TMRank := Rank
         CurrentLeaderLastResponse := {BootTime.getReferenceTime}
         for I in {Dictionary.items LocalStore} do
            {TheTimer startTrigger(@VotingPeriod timeoutPoll(I.key))}
         end
      end

      proc {VoteAck voteAck(key:Key vote:_ tid:_ tmid:_ rtm:TM tag:trapp)}
         VotesAcks.Key := TM | VotesAcks.Key
         /*if {Not @Done} then
         	{CheckDecision}
         end*/
      end

      proc {Vote FullVote}
         if @Leader\=noref andthen FullVote.leader.rank == @Leader.rank then
            Key = FullVote.key
            in
            Votes.Key   := FullVote | Votes.Key
            TPs.Key     := FullVote.tp | TPs.Key
            %{System.show 'Debug: Received vote '#FullVote.vote#' for key '#FullVote.key#
	    %	' from TP with peer id '#FullVote.tp.ref.id#' at '#@NodeRef.id#' for leader with peer id '#@Leader.ref.id}
            if {Not @Done} andthen VotingPolls.Key == open then
               {CheckConsensus Key}
            end
        else
            {@MsgLayer dsend(to:FullVote.tp.ref leaderChanged(tid:     Tid
                                                	      tpid:    FullVote.tp.id
                                                	      tag:     trapp
                                                             ))}
        end    
      end

      %% --- Masking Transaction operations write/read/erase ----
      proc {PreWrite Event}
         case Event
         of write(Key Val) then
            {Write write(s:NO_SECRET k:Key v:Val r:_)}
         [] write(k:Key v:Val r:Result) then
            {Write write(s:NO_SECRET k:Key v:Val r:Result)}
         [] write(s:Secret k:Key v:Val r:Result) then
            {Write write(s:Secret k:Key v:Val r:Result)}
         else
            raise
               error(wrong_invocation(event:write
                                      found:Event
                                      mustbe:write(s:secret
                                                   k:key
                                                   v:value
                                                   r:result)))
            end
         end
      end

      proc {PreRead Event}
         case Event
         of read(Key Result) then
            {Read read(k:Key v:Result)}
         [] read(k:Key v:Result) then
            {Read read(k:Key v:Result)}
         [] read(s:_ k:Key v:Result) then
            {Debug "Transaction Warning: secrets are not used for reading"}
            {Read read(k:Key v:Result)}
         else
            raise
               error(wrong_invocation(event:read
                                      found:Event
                                      mustbe:read(k:key v:result)))
            end
         end
      end

      proc {PreErase Event}
         case Event
         of erase(Key) then
            {Erase erase(s:NO_SECRET k:Key r:_)}
         [] erase(k:Key r:Result) then
            {Erase erase(s:NO_SECRET k:Key r:Result)}
         [] erase(s:Secret k:Key r:Result) then
            {Erase erase(s:Secret k:Key r:Result)}
         else
            raise
               error(wrong_invocation(event:erase
                                      found:Event
                                      mustbe:erase(s:secret
                                                   k:key
                                                   r:result)))
            end
         end
      end
      %% --- End of Masking -------------------------------------------------


      %% --- Operations for the client --------------------------------------
      proc {Abort Msg}
         if @Done then
            {AbortTransaction}
         end
      end

      proc {GetLocks getLocks(Keys)}
         LockHolderPbeer = @NodeRef
         for K in Keys do 
            Item = {GetItem K}
            in
            {Wait Item}
            LocalStore.K := {Record.adjoin LocalStore.K item(op:write client:@NodeRef.id)}
         end
         %{System.show 'Debug: Initiating RTMs for '#Keys#' at leader with peer id '#@NodeRef.id}
         {@Replica reverseBulk(to:@NodeRef.id 
                                initRTM(leader:  @Leader
                                        tid:     Tid
                                        lockkey: LockKey
                                        protocol:eagerpaxos
                                        client:  Client
                                        store:   {Dictionary.entries LocalStore}
                                        lockholder: LockHolderPbeer
                                        tag:     trapp
                                        ))} 
        {TheTimer startTrigger(@VotingPeriod timeoutRTMs)}
      end

      proc {Erase erase(k:Key s:Secret r:Result)}
         {Write write(k:Key v:NO_VALUE s:Secret r:Result)}
      end

      proc {Read read(k:Key v:?Val)}
         Val   = {GetItem Key}.value
      end

      proc {Write write(k:Key v:Val s:Secret r:Result)}
         Item
      in
         Item = {GetItem Key}
         {Wait Item}
         %% Either creation of item orelse rewrite with correct secret
         if Item.version == 0
            orelse Item.secret == Secret
            orelse Item.value == NO_VALUE %% The value was erased
            then
            	LocalStore.Key :=  item(key:     Key
                                    value:   Val 
                                    secret:  Secret
                                    version: Item.version+1
                                    readers: Item.readers 
                                    op:      write)
         else %% Attempt rewrite with wrong secret
            Result = abort(BAD_SECRET)
            {Abort abort(BAD_SECRET)}
         end
      end

      proc {PropagateNewValues commitTransaction(KeyValuePairs)}
        proc {CommunicateAnUpdate PairList}
           case PairList
           of (Key#Value)|MoreValues then
             Item
             in
             Item = {GetItem Key} 
             %% Propagate to rTMs
             LocalStore.Key:={Record.adjoinAt Item value Value}
            
             for RTM in @RTMs do
                  {@MsgLayer dsend(to:RTM.ref
                             updateRTM(tid:Tid tmid:RTM.id item:{Record.adjoinAt Item
                                                                 value
                                                                 Value} tag:trapp))}
             end
             %% Propagate to the TPs
             {@Replica bulk(to:{Utils.hash Key @MaxKey}
                           			update(tid:     Tid
                                                lockkey: LockKey
                           		     	item:{Record.adjoinAt Item
                                                                value
                                                                Value}
                           		     	protocol:eagerpaxos
                           		     	tag:     trapp
                           		     	))} 
             /*for TP in TPs.Key do
               {@MsgLayer dsend(to:TP.ref update(item:{Record.adjoinAt Item
                                                                value
                                                                Value}
                                                tid:     Tid
                                                tpid:    TP.id
                                                tag:     trapp
                                                ))}
             end*/
            
             %% Propagate to the readers
             for Peer in Item.readers do
               {@MsgLayer dsend(to:Peer notifyReaderUpdate(key:Key val:Value tag:trapp))}
             end
             {CommunicateAnUpdate MoreValues}
           [] nil then
             skip
           end
        end
        in
        %{System.show 'Debug:Updating Keys '#KeyValuePairs#' at leader with peer id:'#@Leader.ref.id}
        {CommunicateAnUpdate KeyValuePairs}
        for TM in @RTMs do
              {@MsgLayer dsend(to:TM.ref notifyTermination(tid:     Tid
                                                	   tmid:    TM.id
                                                           tag:     trapp))}
        end
        {@Listener deleteTM(tid:Tid tmid:Id tag:trapp)}
        {Suicide}
      end

      %% ---Failure Handlers------------------------------------------------
      proc {StartRound S}
         OldLeader = @Leader
         in
         CurrentRound := S
         if @Leader \= noref then
            RTMs := @Leader|@RTMs
         end
         Leader:=noref
         
         if @TMRank \= S then
                for TM in @RTMs do
                   if S == TM.rank then
                      {@MsgLayer dsend(to:TM.ref startLeader(rtm:tm(ref:@NodeRef id:Id rank:@TMRank)
                                                     leaderRank:S 
                                                     tmid:TM.id
                                                     tid: Tid
                                                     tag: trapp))}
                   end
                end
         	
                {TheTimer startTrigger(@LEPeriod timeoutLeader)}
         else
                Role := leader
                Leader := tm(ref:@NodeRef id:Id rank:@TMRank)
               
                for TM in @RTMs do
                   if TM.rank \= S then
                      {@MsgLayer dsend(to:TM.ref okLeader(leader:@Leader
                                                     tmid:TM.id
                                                     tid: Tid
                                                     tag: trapp))}   
                   end
                end
                if {Not @Done} then
                	RTMs := nil
                         %% Notify all previous TPs to commit suicide
               		for I in {Dictionary.items LocalStore} do
                    		{@Replica bulk(to:{Utils.hash I.key @MaxKey}
                           			releaseLock(
                           		     	tid:     Tid
                                                lockkey: LockKey
                           		     	item:    I
                           		     	protocol:eagerpaxos
                           		     	tag:     trapp
                           		     	))} 

                   		 /*for TP in TPs.Key do
					{@MsgLayer dsend(to:TP.ref leaderChanged(tid:     Tid
                                                	      tpid:    TP.id
                                                	      tag:     trapp
                                                             ))}
                    		end*/
                	end
                	{TheTimer startTrigger(@VotingPeriod timeoutRTMs)}
		else
                   %% inform client, give new lock
                   if OldLeader \= noref then
                   	{@MsgLayer dsend(to:LockHolderPbeer 
				notifyLockChange(old:OldLeader.ref.id#Tid#OldLeader.id#@PeriodToCommit 
						new:@NodeRef.id#Tid#@Leader.id#@PeriodToCommit tag:trapp))}
		   end
                   {TheTimer startTrigger(@PeriodToCommit timeoutToCommit)}
                   %skip
                end
         end
      end

      proc {DiscardAllVotes}
	for I in {Dictionary.items LocalStore} do
             if I.op==write then
		Votes.(I.key)  := nil
            	TPs.(I.key)    := nil
            	VotesAcks.(I.key) := nil
                VotingPolls.(I.key)  := open
             end
        end
     end

      %% ---Failure Handling Events-----------

      %% ----- local function---------
      proc {InitLeaderChange}
	Suspected := @Leader|@Suspected
        {@MsgLayer dsend(to:@Leader.ref stopLeader(leader:@Leader
                                                     tmid:@Leader.id
                                                     tid: Tid
                                                     tag: trapp))}
        if {List.length @RTMs} == 0 andthen @TMRank == 0 then
            {@Replica quickBulk(to:@NodeRef.id 
                                askRTMResponse(rtm: tm(ref:@NodeRef id:Id)
                                               tid:     Tid
                                               tag:     trapp
                                               ))}
            {TheTimer startTrigger((3*@VotingPeriod) timeoutRTMResponse)} 
        else 
            {StartRound (@CurrentRound mod @RepFactor)+1}
        end
      end

      %%%%%%%%%%%%%

      proc {IsATMCrashed isATMCrashed(Pbeer)}
       	   if @Leader \= noref andthen @Leader.ref.id == Pbeer.id then
              if {Not @Done} then
		if @CurrentLeaderLastResponse == nil orelse 
			{BootTime.getReferenceTime}-@CurrentLeaderLastResponse<Constants.leaderFreshnessPeriod then
		    {TheTimer startTrigger(Constants.leaderFreshnessPeriod timeoutLeaderFreshness(@Leader.rank))}	
                 else
                    {InitLeaderChange}
                 end
              else
                 {TheTimer startTrigger(Constants.leaderFreshnessPeriod timeoutLeaderFreshness(@Leader.rank))}
              end
           else
	        for RTM in @RTMs do
                   if RTM.ref.id == Pbeer.id then
                       Suspected:=RTM|@Suspected
                   end
                end
           end
      end

      proc {StopLeader stopLeader(leader:ALeader tmid:_ tid:_ tag:trapp)}
          if ALeader.rank >= @CurrentRound then
             Role := rtm
             {StartRound (ALeader.rank mod @RepFactor)+1}
          end
      end

      proc {StartLeader startLeader(rtm:ATM leaderRank:K tmid:_ tid:_ tag:trapp)}
         if K>@CurrentRound then
            {StartRound (K mod @RepFactor)}
         else
            if K==@CurrentRound then
                Role := leader
                Leader := tm(ref:@NodeRef id:Id rank:@TMRank)
                {@MsgLayer dsend(to:ATM.ref okLeader(leader:@Leader
                                                     tmid:ATM.id
                                                     tid: Tid
                                                     tag: trapp))}			
            end
         end
      end

      proc {OkLeader okLeader(leader:NewLeader tmid:_ tid:_ tag:trapp)}
          if NewLeader.rank==@CurrentRound then
              if @Leader==noref then
                 Leader:=NewLeader
                 CurrentLeaderLastResponse := {BootTime.getReferenceTime}
                 {@MsgLayer dsend(to:@Leader.ref registerRTM(rtm: tm(ref:@NodeRef id:Id rank:@TMRank)
                                                     tmid:@Leader.id
                                                     tid: Tid
                                                     tag: trapp))}
                 if {Not @Done} then
                    {DiscardAllVotes}
                 end
             end
         else
             if NewLeader.rank > @CurrentRound then
                {StartRound (NewLeader.rank mod @RepFactor)}
             end
        end
     end

     proc {AskRTMResponse askRTMResponse(rtm:ARTM hkey:_ tid:_ tag:trapp)}
         {@MsgLayer dsend(to:ARTM.ref aRTMResponse(rtm: tm(ref:@NodeRef id:Id rank:@TMRank)
                                                   rtms: @RTMs
                                                   leader:@Leader
                                                   tmid:ARTM.id
                                                   tid: Tid
                                                   tag: trapp))}
      end

      proc {ARTMResponse aRTMResponse(rtm:ATM rtms:RTMSet leader:ALeader tmid:_ tid:_ tag:trapp)}
	if ALeader\=noref andthen @Leader\=noref andthen ALeader.rank == @Leader.rank 
           andthen {List.length RTMSet}==0 andthen ATM.rank==0 then
	   RTMs := ATM|@RTMs
           if {List.length @RTMs} == @RepFactor-2 then 
              Lowest = {NewCell noref} 
              NewRTMs = {NewCell nil}
              in
              for RTM in @RTMs do
              	if @Lowest==noref orelse RTM.ref.id < @Lowest.ref.id then
                   Lowest:=RTM
                end
              end
              RTMCount:=2
              Leader := {Record.adjoinAt Lowest rank @RTMCount}
              RTMCount := @RTMCount + 1
              if Leader.ref.id == @NodeRef.id then
                  Role:=leader
                  TMRank:=@Leader.rank
                  for RTM in @RTMs do 
                     NewRTMs := {Record.adjoinAt RTM rank @RTMCount}|@NewRTMs
                     RTMCount := @RTMCount + 1  
                  end 
                  RTMs:=NewRTMs
                  {StartValidation} 
              end
           end 
        elseif {List.length @RTMs}==0 andthen @TMRank==0 then
           Leader:=ALeader
           RTMs := RTMSet
           for RTM in @RTMs do
              if RTM.id == Id then
                 TMRank:=RTM.rank
              end
           end
           if @Leader==noref then
               {StartRound (@CurrentRound mod @RepFactor)+1}
           else
               for RTM in @Suspected do
                  if RTM\=noref andthen @Leader\=noref andthen RTM.ref.id == @Leader.ref.id then
                       {StartRound (@Leader.rank mod @RepFactor)+1}
                  end
               end
           end
       end
      end


      %% --- Various --------------------------------------------------------

      proc {GetId getId(I)}
         I = Id
      end

      proc {GetTid getTid(I)}
         I = Tid
      end

      proc {GetLockPeriod getLockPeriod(Period)}
         Period = @PeriodToCommit
      end

      proc {SetReplica setReplica(ReplicaMan)}
         Replica     := ReplicaMan
         RepFactor   := {@Replica getFactor($)}
      end

      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
         if @Role == leader then
            MaxInt = {Property.get 'limits.int.max'}
            in
            Leader := tm(ref:@NodeRef id:Id rank:1)
            TMRank := 1
            LockKey = @NodeRef.id#{Random.urandInt 1 MaxInt}#{BootTime.getReferenceTime}
         end
      end

      proc {SetVotingPeriod setVotingPeriod(Period)}
         VotingPeriod := Period
      end

      proc {SetLockingPeriod setLockingPeriod(Period)}
         PeriodToCommit := Period
      end

      proc {TimeoutPoll timeoutPoll(Key)}
         if {Dictionary.member VotingPolls Key} andthen VotingPolls.Key == open then
            VotingPolls.Key := close
            {CheckConsensus Key}
         end
      end

      proc {TimeoutRTMs timeoutRTMs}
         if @Leader\=noref andthen @Leader.id==Id andthen {List.length @RTMs} < @RepFactor-1 then
            % Didn't receive response from all RTMs
            if {Not @Done} then
		FinalDecision = abort
            	Done := true
                {SpreadDecision FinalDecision}
            end
         end
      end

      proc {TimeoutLeader timeoutLeader}
         if @Leader == noref then
            for TM in @RTMs do
                if @CurrentRound == TM.rank then
            		{@MsgLayer dsend(to:TM.ref stopLeader(leader:TM
                                                     tmid:TM.id
                                                     tid: Tid
                                                     tag: trapp))}
                end
            end
            {StartRound (@CurrentRound mod @RepFactor)+1} 
         end
      end

      proc {TimeoutLeaderFreshness timeoutLeaderFreshness(LeaderRank)}
     	if LeaderRank == @Leader.rank then
	  if @CurrentLeaderLastResponse == nil orelse 
			{BootTime.getReferenceTime}-@CurrentLeaderLastResponse>Constants.leaderFreshnessPeriod then
             {InitLeaderChange}
          end
        end
      end

      proc {TimeoutRTMResponse timeoutRTMResponse}
         if {Not @Done} andthen {List.length @RTMs}==0 then
            FinalDecision = abort
            Done := true
            {SpreadDecision FinalDecision}
         elseif {Not @Done} andthen {List.length @RTMs} < @RepFactor-2 andthen @TMRank==0 then
             Lowest = {NewCell noref}  
             in
             for RTM in @RTMs do
              	if @Lowest==noref orelse RTM.ref.id < @Lowest.ref.id then
                   Lowest:=RTM
                end
             end
             RTMCount:=2
             Leader := {Record.adjoinAt Lowest rank @RTMCount}
             RTMCount := @RTMCount + 1
             if @Leader.ref.id == @NodeRef.id then
                Role:=leader
                TMRank:=@Leader.rank
                FinalDecision = abort
                Done := true
                {SpreadDecision FinalDecision}
            end
         end
      end

      proc {TimeoutToCommit timeoutToCommit}
         % Ask TPs to release lock and inform client
         if @Leader\=noref andthen @Leader.id==Id then
            {@MsgLayer dsend(to:LockHolderPbeer 
			notifyLockRelease(lockKey:@NodeRef.id#Tid#@Leader.id#@PeriodToCommit tag:trapp))}
            {AbortTransaction}
         end
      end

     proc {DestroyRTM Event}
         {Suicide}
      end

      Events = events(
                     %% Operations for the client
                     abort:                   Abort
                     commitTransaction:        PropagateNewValues
                     getLocks:      GetLocks
                     erase:         PreErase
                     read:          PreRead
                     write:         PreWrite
                     %becomeReader:  BecomeReader
                     %% Interaction with rTMs
                     initRTM:       InitRTM
                     registerRTM:   RegisterRTM
                     rtms:          SetRTMs
                     startLeader:   StartLeader  % Leader Election Event
                     stopLeader:    StopLeader   % Leader Election Event
                     okLeader:      OkLeader     % Leader Election Event
                     setFinal:      SetFinal
                     voteAck:       VoteAck
                     notifyTermination: NotifyTermination
                     updateRTM:     UpdateRTM
                     %% Interaction with TPs
                     vote:          Vote
                     %ackNewReader:  AckNewReader
                     %% Various
                     getId:         GetId
                     getTid:        GetTid
                     getLockPeriod: GetLockPeriod
                     setReplica:    SetReplica
                     setMsgLayer:   SetMsgLayer
                     setVotingPeriod:SetVotingPeriod
                     setLockingPeriod: SetLockingPeriod
                     timeoutPoll:   TimeoutPoll
                     timeoutLeader: TimeoutLeader
                     timeoutRTMs:   TimeoutRTMs
                     timeoutRTMResponse: TimeoutRTMResponse
                     timeoutToCommit:  TimeoutToCommit
                     timeoutLeaderFreshness: TimeoutLeaderFreshness
                     isATMCrashed:  IsATMCrashed
                     askRTMResponse: AskRTMResponse
                     aRTMResponse:   ARTMResponse
                     destroyRTM:     DestroyRTM
                     )
     in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Suicide  = FullComponent.killer
         Listener = FullComponent.listener
      end
      MsgLayer    = {NewCell Component.dummy}
      Replica     = {NewCell Component.dummy}      
      TheTimer    = {Timer.new}
      {TheTimer setListener(Self)}

      Client      = Args.client
      Id          = {Name.new}
      RepFactor   = {NewCell 0}
      NodeRef     = {NewCell noref}
      Votes       = {Dictionary.new}
      VotingPolls = {Dictionary.new}
      VotesAcks   = {Dictionary.new}
      VotingPeriod= {NewCell Constants.eagerPaxosVotingPeriod}
      PeriodToCommit = {NewCell Constants.eagerPaxosLockPeriod}
      TMRank 	  = {NewCell 0}
      LocalStore  = {Dictionary.new}

    
      TPs         = {Dictionary.new}
      RTMs        = {NewCell nil}
      VotedItems  = {NewCell nil}
      Done        = {NewCell false}
      MaxKey      = {NewCell Args.maxKey}
      Role        = {NewCell Args.role}

      CurrentRound = {NewCell 1}
      LEPeriod     = {NewCell Constants.paxosLeaderElectionPeriod}
      Suspected   = {NewCell nil}

      CurrentLeaderLastResponse = {NewCell nil}
      
      if @Role == leader then
         Tid         = Args.transId
         Leader      = {NewCell noref}
         RTMCount    = {NewCell 2}
      end

      Self
   end
end  
