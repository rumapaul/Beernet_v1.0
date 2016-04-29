/*-------------------------------------------------------------------------
 *
 * TwoPhase-TM.oz
 *
 *    Transaction Manager for the Two-Phase Commit Protocol    
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
 *    Implementation of the classical two-phase commit protocol for replicated
 *    databases. It relies strongly on the transaction manager.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Component      at '../../corecomp/Component.ozf'
   Utils          at '../../utils/Misc.ozf'
export
   New
define

   fun {New Args}
      Self
      Suicide
      MsgLayer
      Replica

      Id             % Id of the transaction manager object
      Tid            % Id of the transaction
      RepFactor      % Replication Factor
      NodeRef        % Node Reference
      FinalDecision  % Decision taken after collecting votes
      LocalStore     % Stores involve items with their new values and operation
      Votes          % To collect votes from Transaction Participants
      Acks           % To collect final acknoweledgements from TPs
      TPs            % Direct reference to transaction participants
      VotedItems     % Collect items once enough votes are received 
      AckedItems     % Collect items once enough acks are received 
      Done           % Flag to know when we are done
      MaxKey         % To use the hash function

      %% --- Util functions ---
      fun lazy {GetRemote Key}
         Item
         RemoteItem
      in
         RemoteItem = {@Replica getOne(Key $ trapp)}
         Item = if RemoteItem \= 'NOT_FOUND' then
                   {Record.adjoinAt RemoteItem op read}
                else
                   item(key:Key value:RemoteItem version:0 readers:nil op:read)
                end
         LocalStore.Key := Item 
         Item
      end

      fun {GetItem Key}
         {Dictionary.condGet LocalStore Key {GetRemote Key}}
      end

      fun {GotAllBrewed}
         fun {CheckAllVotes TheVotes C}
            case TheVotes
            of Vote|MoreVotes then
               if Vote.vote == brewed then
                  {CheckAllVotes MoreVotes C+1}
               else
                  false
               end
            [] nil then 
               C == @RepFactor
            end
         end
         fun {CheckAllKeys Keys}
            case Keys
            of Key|MoreKeys then
               if {CheckAllVotes Votes.Key 0} then
                  {CheckAllKeys MoreKeys}
               else
                  false
               end
            [] nil then
               true
            end
         end
      in
         {CheckAllKeys {Dictionary.keys Votes}}
      end

      proc {SpreadDecision Decision}
         for Key in {Dictionary.keys Votes} do
            for TP in TPs.Key do
               {@MsgLayer dsend(to:TP.ref final(decision:Decision
                                                tid:     Tid
                                                tpid:    TP.id
                                                tag:     trapp
                                                ))}
            end
         end
      end

      %% --- Events ---
      %% --- Operations for the client ---
      proc {Abort abort}
         try
            {Port.send Args.client abort}
         catch _ then
            %% TODO: improve exception handling
            skip
         end
         Done := true
         {Suicide}
      end

      proc {Commit commit}
         %% - Loop over the items on LocalStore.
         %% - Send 'brew' (prepare) to TPs for every item
         %% - Collect responses from TPs (from all, this 2PC
         %% - Decide on commit or abort
         %% - Spread decision to TPs
         for I in {Dictionary.items LocalStore} do
            {@Replica bulk(to:{Utils.hash I.key @MaxKey}
                           brew(tm:   @NodeRef
                                tid:  Tid
                                tmid: Id
                                item: I
                                protocol:twophase
                                tag:  trapp
                                ))} 
            Votes.(I.key)  := nil
            Acks.(I.key)   := nil
            TPs.(I.key)    := nil
         end
      end

      proc {Read read(Key ?Val)}
         Val   = {GetItem Key}.value
      end

      proc {Write write(Key Val)}
         Item
      in
         Item = {GetItem Key}
         LocalStore.Key :=  item(key:Key
                                 value:Val 
                                 version:Item.version + 1
                                 readers:Item.readers 
                                 op:write)
      end

      %% --- Interaction with TPs ---

      proc {Ack ack(key:Key tid:_ tmid:_ tp:TP tag:trapp)}
         Acks.Key := TP | Acks.Key
         if {Length Acks.Key} == @RepFactor then
            AckedItems := Key | @AckedItems
            if {Length @AckedItems} == {Length {Dictionary.keys Votes}} then
               try
                  {Port.send Args.client FinalDecision}
               catch _ then
                  %% TODO: improve exception handling
                  skip
               end
               Done := true
            end
         end
      end

      proc {Vote FullVote}
         Key = FullVote.key
      in
         Votes.Key   := FullVote | Votes.Key
         TPs.Key     := FullVote.tp | TPs.Key
         if {Length TPs.Key} == @RepFactor then
            VotedItems := Key | @VotedItems
            if {Length @VotedItems} == {Length {Dictionary.keys Votes}} then
               %% Collected everything
               FinalDecision = if {GotAllBrewed} then commit else abort end
               {SpreadDecision FinalDecision}
            end
         end
      end

      %% --- Various ---

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
      end

      Events = events(
                     %% Operations for the client
                     abort:         Abort
                     commit:        Commit
                     read:          Read
                     write:         Write
                     %% Interaction with TPs
                     ack:           Ack
                     vote:          Vote
                     %% Various
                     getId:         GetId
                     getTid:        GetTid
                     setReplica:    SetReplica
                     setMsgLayer:   SetMsgLayer
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self           = FullComponent.trigger
         Suicide        = FullComponent.killer
      end
      MsgLayer    = {NewCell Component.dummy}
      Replica     = {NewCell Component.dummy}      

      Id          = {Name.new}
      Tid         = {Name.new}
      RepFactor   = {NewCell 0}
      NodeRef     = {NewCell noref}
      LocalStore  = {Dictionary.new}
      Votes       = {Dictionary.new}
      Acks        = {Dictionary.new}
      TPs         = {Dictionary.new}
      VotedItems  = {NewCell nil}
      AckedItems  = {NewCell nil}
      Done        = {NewCell false}
      MaxKey      = {NewCell Args.MaxKey}

      Self
   end
end  

