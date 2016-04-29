/*-------------------------------------------------------------------------
 *
 * Paxos-TP.oz
 *
 *    Transaction Participant for the Paxos Consensus Commit Protocol    
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *             Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 503 $ $Author: ruma $
 *
 *    $Date: 2016-04-25 18:48:10 +0200 (Mon, 25 April 2016) $
 *
 * NOTES
 *
 *    Implementation of transaction participant (TP) role on the paxos
 *    consensus algorithm. This is one of the replicas of the protocol. If the
 *    majority of TPs survives the transaction, the transaction will finish. 
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Component      at '../../corecomp/Component.ozf'
   Constants      at '../../commons/Constants.ozf'

export
   New
define

   NOT_FOUND   = Constants.notFound
   NO_VALUE    = Constants.noValue

   fun {New CallArgs}
      Self
      %Listener
      Suicide
      MsgLayer
      NodeRef
      DB

      Id
      NewItem
      Leader
      RTMs

      %% === Events =========================================================

      %% --- Interaction with TPs ---
      proc {Brew brew(hkey:   HKey
                      leader: TheLeader
                      rtms:   TheRTMs
                      tid:    Tid
                      lockkey: ALockKey  %% using a generated id instead of Tid as locker to avoid name variable problem
                      item:   TrItem 
                      protocol:_ 
                      tag:trapp)}
         Tmp
         DBItem
         Vote
      in 
         RTMs     = TheRTMs
         NewItem  = item(hkey:HKey item:TrItem tid:Tid lockkey:ALockKey)
         Leader   := TheLeader
         Tmp      = {@DB get(HKey TrItem.key $)}
         DBItem   = if Tmp == NOT_FOUND then 	%orelse Tmp.value == NO_VALUE then
                        item(key:      TrItem.key
                             secret:   TrItem.secret
                             value:    NO_VALUE 
                             version:  0
                             readers:  nil
                             locked:   false)
                    else
                       Tmp
                    end
         %% Brewing vote - tmid needs to be added before sending
         Vote = vote(vote:    _
                     key:     TrItem.key 
                     secret:  TrItem.secret
                     version: DBItem.version 
                     leader:  TheLeader
                     tid:     Tid 
                     tp:      tp(id:Id ref:@NodeRef)
                     tag:     trapp)
         if TrItem.version >= DBItem.version
            andthen TrItem.secret == DBItem.secret
            %andthen {Not DBItem.locked} then
            andthen ({Not DBItem.locked} orelse ({HasFeature DBItem locker} andthen DBItem.locker==ALockKey)) then
            	Vote.vote = brewed
            	{@DB put(HKey TrItem.key {Adjoin DBItem item(locked:true
                                                          locker:ALockKey)})}
         else
            Vote.vote = denied
         end
         {@MsgLayer dsend(to:@Leader.ref 
                          {Record.adjoinAt Vote tmid @Leader.id})}
         %{System.show 'Debug(paxos):Sent Vote '#Vote.vote#' for key '#TrItem.key#' for leader with peer id '#@Leader.ref.id}
         /*for TM in RTMs do
            {@MsgLayer dsend(to:TM.ref {Record.adjoinAt Vote tmid TM.id})}
         end*/
      end

      proc {Abort abort}
         DBItem
      in
         DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
         %{System.show 'Debug(paxos): Reached Abort for key '#NewItem.item.key}
         if DBItem \= NOT_FOUND andthen DBItem.locked 
              andthen {HasFeature DBItem locker} andthen DBItem.locker == NewItem.lockkey then
            {PutItemAndAck DBItem}
         else
            %{@DB delete(NewItem.hkey NewItem.item.key)}
            {AckDecision NewItem.item}
         end
      end

      proc {Commit commit}
         DBItem
      	 in
         DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
         if DBItem \= NOT_FOUND andthen DBItem.locked 
              andthen {HasFeature DBItem locker} andthen DBItem.locker == NewItem.lockkey then
         	{PutItemAndAck NewItem.item}
         end
      end

      proc {PutItemAndAck Item}
         %DBItem
         %ItemToUpload
         %in
         %DBItem      = {@DB get(NewItem.hkey Item.key $)}
         %ItemToUpload = {Record.adjoinAt Item readers DBItem.readers}
         %{System.show 'Debug(paxos): Putting value'#Item.value#' for key '#Item.key#' at TP with peer id '#@NodeRef.id}
         {@DB put(NewItem.hkey Item.key item(key:Item.key
                                             value:Item.value
                                             secret:Item.secret
                                             version:Item.version
                                             readers:Item.readers
                                             locked:false
                                             locker:none))}
         {AckDecision Item}
      end

      proc {AckDecision Item}
         AckMessage = ack(key: Item.key
                          tid: NewItem.tid
                          tp:  tp(id:Id ref:@NodeRef)
                          tag: trapp)
         in
         {@MsgLayer dsend(to:@Leader.ref {Record.adjoinAt AckMessage tmid @Leader.id})}
         for TM in RTMs do
            {@MsgLayer dsend(to:TM.ref {Record.adjoinAt AckMessage tmid TM.id})}
         end
         {Suicide} 
      end

      proc {LeaderChanged leaderChanged}
         DBItem
         in
         DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
         %{System.show 'Debug(paxos): Reached Leader Change Event for key '#NewItem.item.key}
         if DBItem \= NOT_FOUND then
            {@DB put(NewItem.hkey DBItem.key {Record.adjoinAt DBItem locked false})}
         %else
         %   {@DB delete(NewItem.hkey NewItem.item.key)}
         end
         {Suicide} 
      end

      proc {NewReader newReader(hkey:   HKey
                      leader: TheLeader
                      tid:    Tid
                      readerpeer: ReaderPeer
                      itemkey: Key 
                      protocol:_ 
                      tag:trapp)}
          DBItem
          NewReaders
          Result
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
          DBItem      = {@DB get(HKey Key $)}
          if DBItem == NOT_FOUND orelse DBItem.value == NO_VALUE then
		Result = not_success 
          elseif {Not DBItem.locked} then
             if {Not {IsInList DBItem.readers ReaderPeer}} then
                NewReaders = ReaderPeer|(DBItem.readers)
                {@DB put(HKey Key {Record.adjoinAt DBItem readers NewReaders})}
             end
     	     Result = success
          else
             Result = not_success    
          end
          {@MsgLayer dsend(to:TheLeader.ref ackNewReader(key: Key
                                                         result: Result
                                                         tmid: TheLeader.id
                                                         tid: Tid
                                                         tp:  tp(id:Id ref:@NodeRef)
                                                         tag: trapp))}   
          {Suicide}     
     end

      %% --- Various --------------------------------------------------------

      proc {GetId getId(I)}
         I = Id
      end

      proc {SetDB setDB(ADB)}
         DB := ADB
      end

      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
      end

      Events = events(
                     %% Interaction with TM
                     brew:          Brew
                     abort:         Abort
                     commit:        Commit
                     leaderChanged: LeaderChanged
                     newReader:     NewReader
                     %% Various
                     getId:         GetId
                     setDB:        SetDB
                     setMsgLayer:   SetMsgLayer
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Suicide  = FullComponent.killer
         %Listener = FullComponent.listener
      end
      MsgLayer = {NewCell Component.dummy}
      DB   = {NewCell Component.dummy}      

      Id       = {Name.new}
      NodeRef  = {NewCell noref}
      Leader   = {NewCell noleader}

      Self
   end
end  

