/*-------------------------------------------------------------------------
 *
 * ValueSet-TM.oz
 *
 *    Transaction Manager for the Key/Value-Set abstraction   
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 406 $ $Author: boriss $
 *
 *    $Date: 2011-05-23 12:03:08 +0200 (Mon, 23 May 2011) $
 *
 * NOTES
 *
 *    Implementation of semi-lock-free protocol to add and remove elements from
 *    a key/value-set, using replicated transaction managers à la Paxos
 *    consensus algorithm.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
%   System
   Component      at '../../corecomp/Component.ozf'
   Constants      at '../../commons/Constants.ozf'
   HashedList     at '../../utils/HashedList.ozf'
   Utils          at '../../utils/Misc.ozf'
   Timer          at '../../timer/Timer.ozf'
export
   New
define

   NO_VALUE    = Constants.noValue

   fun {New Args}
      Self
      %Listener
      MsgLayer
      Replica
      TheTimer

      Client         % Client port to communicate final decision
      Id             % Id of the transaction manager object
      Tid            % Id of the transaction
      RepFactor      % Replication Factor
      NodeRef        % Node Reference
      FinalDecision  % Decision taken after collecting votes
      OutCome        % Decision send to the Clien
      Leader         % Transaction Leader
      LocalStore     % Stores involve items with their new values and operation
      Votes          % To collect votes from Transaction Participants
      VotingPeriod   % Time to vote for TPs
      VotingPolls    % Register time for voting
      Acks           % To collect final acknoweledgements from TPs
      Role           % Role of the TM: leader or rtm
      RTMs           % Set of replicated transaction managers rTMs
      VotesAcks      % Collect decided items from rTMs
      TPs            % Direct reference to transaction participants
      VotedItems     % Collect items once enough votes are received 
      %AckedItems     % Collect items once enough acks are received 
      Done           % Flag to know when we are done
      MaxHash        % Just to make list more efficient
      MaxKey         % To use the hash function

      %% --- Util functions -------------------------------------------------

      %% AnyMajority uses a timer to wait for all TPs instead of claiming
      %% majority as soon as it is reached.
      fun {AnyMajority Key}
         fun {CountVotes L Acc}
            case L
            of Vote|MoreVotes then
               {CountVotes MoreVotes {AdjoinAt Acc Vote.vote Acc.(Vote.vote)+1}}
            [] nil then
               Acc
            end
         end
         fun {DecideOnResults Results Arity}
            case Arity
            of Vote|MoreVotes then
               if Results.Vote > @RepFactor div 2 then
                  Vote
               else
                  {DecideOnResults Results MoreVotes}
               end
            [] nil then
               denied
            end
         end
         TheVotes
         Results
      in
         TheVotes = Votes.Key
         if VotingPolls.Key == open andthen {Length TheVotes} < @RepFactor then
            none
         else
            VotingPolls.Key := close
            Results = {CountVotes TheVotes acc(brewed:0
                                               conflict:0
                                               duplicated:0
                                               bad_secret:0
                                               not_found:0)}
            {DecideOnResults Results {Record.arity Results}}
         end
      end

      proc {CheckDecision}
         if {Length @VotedItems} == {Length {Dictionary.keys Votes}} then
            %% Collected everything
            if {EnoughRTMacks {Dictionary.keys VotesAcks}} then
               FinalDecision = if {GotAll brewed} then commit else abort end
               OutCome  = if FinalDecision == commit
                          orelse {GotAll duplicated} then
                             commit
                           else
                              abort
                           end
               Done := true
               {SpreadDecision FinalDecision}
               %% Send to the Client
               try
                  {Port.send Client OutCome}
               catch _ then
                  %% TODO: improve exception handling
                  skip
               end
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

      fun {GotAll What}
         fun {Loop L}
            case L
            of Vote|MoreVotes then
               if Vote.consensus == What then
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

      proc {StartValidation}
         %% Notify all rTMs
         for RTM in @RTMs do
            {@MsgLayer dsend(to:RTM.ref
                             rtms(@RTMs tid:Tid tmid:RTM.id tag:trapp))}
         end
         %% Initiate TPs per each item. Ask them to vote
         for I in {Dictionary.items LocalStore} do
            {@Replica bulk(to:{Utils.hash I.key @MaxKey}
                           brew(leader:  @Leader
                                rtms:    @RTMs
                                tid:     Tid
                                item:    I
                                protocol:valueset
                                tag:     trapp
                                ))} 
            Votes.(I.key)  := nil
            Acks.(I.key)   := nil
            TPs.(I.key)    := nil
            VotesAcks.(I.key) := nil
         end
         %% Open VotingPolls and launch timers
         for I in {Dictionary.items LocalStore} do
            VotingPolls.(I.key) := open
            {TheTimer startTrigger(@VotingPeriod timeoutPoll(I.key))}
         end
      end

      proc {SpreadDecision Decision}
         %% Send to all TPs
         for Key in {Dictionary.keys Votes} do
            for TP in TPs.Key do
               {@MsgLayer dsend(to:TP.ref final(decision:Decision
                                                tid:     Tid
                                                tpid:    TP.id
                                                tag:     trapp
                                                ))}
            end
         end
         %% Send to all rTMs
         for TM in @RTMs do
            {@MsgLayer dsend(to:TM.ref setFinal(decision:Decision
                                                tid:     Tid
                                                tmid:    TM.id
                                                tag:     trapp))}
         end
      end

      fun {FilterSets Sets}
         fun {FilterSet Ops Adds Rems}
            case Ops
            of Op|MoreOps then
               if Op.status == ok then
                  Hop = {Utils.hash Op MaxHash}
               in
                  if Op.op == add then
                     {FilterSet MoreOps {HashedList.add Adds Op Hop} Rems}
                  else
                     {FilterSet MoreOps Adds {HashedList.add Rems Op Hop}}
                  end
               else
                  {FilterSet MoreOps Adds Rems}
               end
            [] nil then
               [Adds Rems]
            end
         end
         fun {FilterLoop Sets Adds Rems}
            case Sets
            of Set|MoreSets then
               MoreAdds MoreRems
            in
               [MoreAdds MoreRems] = {FilterSet {Record.toList Set} Adds Rems}
               {FilterLoop MoreSets MoreAdds MoreRems}
            [] nil then
               [Adds Rems]
            end
         end
      in
         {FilterLoop Sets nil nil}
      end

      fun {AddValues Adds}
         case Adds
         of Op|MoreAdds then
            Op.value.val|{AddValues MoreAdds}
         [] nil then
            nil
         end
      end

      fun {RemoveAndBuild Vals Rems}
         fun {Remove L E}
            case L
            of H|T then
               %{System.show '---------------------- comparing '#H#E}
               if H==E then
                  T
               else
                  H|{Remove T E}
               end
            [] nil then
               nil
            end
         end
      in
         case Rems
         of Op|MoreRems then
            {RemoveAndBuild {Remove Vals Op.value.val} MoreRems}
         [] nil then
            if Vals == nil then
               empty
            else
               {List.toTuple set Vals}
            end
         end
      end

      fun {MergeSets Sets}
         Adds Rems AddedValues
      in
         [Adds Rems] = {FilterSets Sets}
         AddedValues = {AddValues Adds}
         %{System.show '****** Added:'#AddedValues}
         %{System.show '****** Removing:'#Rems}
         {RemoveAndBuild AddedValues Rems}
      end

      %% === Events =========================================================

      proc {Ack ack(key:Key tp:TP tid:_ tmid:_ tag:trapp)}
         Acks.Key := TP | Acks.Key
      end

      proc {Vote FullVote}
         Key = FullVote.key
         Consensus
      in
         Votes.Key   := FullVote | Votes.Key
         TPs.Key     := FullVote.tp | TPs.Key
         Consensus   = {AnyMajority Key}
         if Consensus \= none then
            VotedItems := vote(key:Key consensus:Consensus) | @VotedItems
            if @Leader.id == Id then
               {CheckDecision}
            else
               {@MsgLayer dsend(to:@Leader.ref
                                voteAck(key:    Key
                                        vote:   Consensus
                                        tid:    Tid
                                        tmid:   @Leader.id
                                        rtm:    @NodeRef
                                        tag:    trapp))}
            end
         elseif Consensus == late andthen @Leader.id == Id then
            thread
               {Wait FinalDecision}
               {@MsgLayer dsend(to:FullVote.tp.ref
                                final(decision: FinalDecision
                                      tid:Tid
                                      tpid:FullVote.tp.id
                                      tag:trapp))}
            end
         end
      end

      proc {VoteAck voteAck(key:Key vote:_ tid:_ tmid:_ rtm:TM tag:trapp)}
         VotesAcks.Key := TM | VotesAcks.Key
         if {Not @Done} then
            {CheckDecision}
         end
      end

      proc {InitRTM initRTM(leader: TheLeader
                            tid:    TransId
                            client: TheClient
                            store:  StoreEntries
                            protocol:_
                            hkey:   _
                            tag:    trapp
                            )}
         Tid         = TransId
         Leader      = {NewCell TheLeader}
         Client      = TheClient
         for Key#I in StoreEntries do
            LocalStore.Key := I
            Votes.(I.key)  := nil
            Acks.(I.key)   := nil
            TPs.(I.key)    := nil
            VotesAcks.(I.key) := nil
            VotingPolls.(I.key) := open
         end
%         {System.show @NodeRef.id#'wanna be rTM.... submitting registration'}
         {@MsgLayer dsend(to:@Leader.ref registerRTM(rtm: tm(ref:@NodeRef id:Id)
                                                     tmid:@Leader.id
                                                     tid: Tid
                                                     tag: trapp))}
      end

      proc {RegisterRTM registerRTM(rtm:NewRTM tmid:_ tid:_ tag:trapp)}
         RTMsize
         Majority
      in
         RTMsize  = {List.length @RTMs}
         Majority = (@RepFactor div 2) + 1 
%         {System.show @NodeRef.id#' getting subscription'#RTMsize#Majority}
         if RTMsize =< Majority then
            RTMs := NewRTM|@RTMs
         end
         if RTMsize+1 == Majority then
%            {System.show 'got majority... going to validate'}
            %% We are done with initialization. We start with validation
            {StartValidation}
         end
      end
         
      proc {SetRTMs rtms(TheRTMs tid:_ tmid:_ tag:trapp)}
         RTMs := TheRTMs
         for I in {Dictionary.items LocalStore} do
            {TheTimer startTrigger(@VotingPeriod timeoutPoll(I.key))}
         end
      end

      proc {SetFinal setFinal(decision:Decision tid:_ tmid:_ tag:trapp)}
         FinalDecision = Decision
      end

      %% --- Single operations --------------------------------------

      %% Val is the value associated to the keys. It contains a list of
      %% operations. This function translates that list into a set of ops.
      fun {OpsListToSet Val}
         Elements
      in
         if Val == NO_VALUE then
            Val
         elseif Val.ops == nil then
            empty
         else
            Elements = {HashedList.getValues Val.ops}
            {List.toTuple set Elements}
         end
      end

      fun {BuildSets OpsLists}
         case OpsLists
         of Ops|MoreOps then
            {OpsListToSet Ops}|{BuildSets MoreOps}
         [] nil then
            nil
         end
      end

      proc {AddRemove Event}
         Op Key
      in
         Op    = {Record.label Event}
         Key   = Event.k
         Client= Event.c
         LocalStore.Key := op(id:   {Name.new}
                              op:   Op
                              key:  Key
                              sec:  Event.s
                              val:  Event.v
                              sval: Event.sv)
%         {System.show 'got the operation... going to bulk'}
         {@Replica  quickBulk(to:@NodeRef.id
                              initRTM(leader:  @Leader
                                      tid:     Tid
                                      protocol:valueset
                                      client:  Client
                                      store:   {Dictionary.entries LocalStore}
                                      tag:     trapp
                                      ))}
      end

      proc {ReadSet readSet(k:Key v:Val)}
         OpsLists Sets
      in
         OpsLists = {@Replica getMajority(Key $ sets)}
         Sets = {BuildSets OpsLists}
         Val = {MergeSets Sets}
      end

      proc {CreateSet createSet(k:Key s:Secret ms:MasterSec c:TheClient)}
         Client = TheClient
         LocalStore.Key := op(id:   {Name.new}
                              op:   createSet
                              key:  Key
                              sec:  Secret
                              msec: MasterSec)
         {@Replica  quickBulk(to:@NodeRef.id
                              initRTM(leader:  @Leader
                                      tid:     Tid
                                      protocol:valueset
                                      client:  Client
                                      store:   {Dictionary.entries LocalStore}
                                      tag:     trapp
                                      ))}
      end

      proc {DestroySet destroySet(k:Key ms:MasterSec c:TheClient)}
         Client = TheClient
         LocalStore.Key := op(id:   {Name.new}
                              op:   destroySet
                              key:  Key
                              msec: MasterSec)
         {@Replica  quickBulk(to:@NodeRef.id
                              initRTM(leader:  @Leader
                                      tid:     Tid
                                      protocol:valueset
                                      client:  Client
                                      store:   {Dictionary.entries LocalStore}
                                      tag:     trapp
                                      ))}
         Client = TheClient
      end

      %% --- Various --------------------------------------------------------

      proc {GetId getId(I)}
         I = Id
      end

      proc {GetTid getTid(I)}
         I = Tid
      end

      proc {SetReplica setReplica(ReplicaMan)}
         Replica     := ReplicaMan
         RepFactor   := {@Replica getFactor($)}
      end

      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
         {Wait @NodeRef}
         if @Role == leader then
            Leader := tm(ref:@NodeRef id:Id)
         end
      end

      proc {SetVotingPeriod setVotingPeriod(Period)}
         VotingPeriod := Period
      end

      proc {TimeoutPoll timeoutPoll(Key)}
         VotingPolls.Key := close
      end

      Events = events(
                     %% Single transaction operations
                     add:           AddRemove
                     remove:        AddRemove
                     readSet:       ReadSet
                     createSet:     CreateSet
                     destroySet:    DestroySet
                     %% Interaction with rTMs
                     initRTM:       InitRTM
                     registerRTM:   RegisterRTM
                     rtms:          SetRTMs
                     setFinal:      SetFinal
                     voteAck:       VoteAck
                     %% Interaction with TPs
                     ack:           Ack
                     vote:          Vote
                     %% Various
                     getId:         GetId
                     getTid:        GetTid
                     setReplica:    SetReplica
                     setMsgLayer:   SetMsgLayer
                     setVotingPeriod:SetVotingPeriod
                     timeoutPoll:   TimeoutPoll
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         %Listener = FullComponent.listener
      end
      MsgLayer    = {NewCell Component.dummy}
      Replica     = {NewCell Component.dummy}      
      TheTimer    = {Timer.new}
      {TheTimer setListener(Self)}

      Id          = {Name.new}
      RepFactor   = {NewCell 0}
      NodeRef     = {NewCell noref}
      Votes       = {Dictionary.new}
      Acks        = {Dictionary.new}
      TPs         = {Dictionary.new}
      VotesAcks   = {Dictionary.new}
      VotingPolls = {Dictionary.new}
      VotingPeriod= {NewCell 3000}
      RTMs        = {NewCell nil}
      VotedItems  = {NewCell nil}
      %AckedItems  = {NewCell nil}
      Done        = {NewCell false}
      MaxHash     = 10676725
      MaxKey      = {NewCell Args.maxKey}
      Role        = {NewCell Args.role}
      LocalStore  = {Dictionary.new}
      if @Role == leader then
         Tid         = {Name.new}
         Leader      = {NewCell noref}
      end

      Self
   end
end  

