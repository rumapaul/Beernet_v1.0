/*-------------------------------------------------------------------------
 *
 * PaxosEager-TP.oz
 *
 *    Transaction Participant for the Eager Paxos Consensus Commit Protocol    
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 10 $ $Author: ruma $
 *
 *    $Date: 2016-04-28 12:07:45 +0200 (Thurs, 28 April 2016) $
 *
 * NOTES
 *
 *    Implementation of transaction participant (TP) role on the Eager paxos
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
                      lockkey: ALockKey
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
         %{System.show 'Debug: Retrieved DBItem '#Tmp#' at TP '#@NodeRef.id}
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
	 %{System.show 'Debug: Decided DBItem '#DBItem#' at TP '#@NodeRef.id}
         %% Brewing vote - tmid needs to be added before sending
         Vote = vote(vote:    _
                     key:     TrItem.key 
                     secret:  TrItem.secret
                     version: DBItem.version 
                     leader:  TheLeader
                     tid:     Tid 
                     lockkey: ALockKey
                     tp:      tp(id:Id ref:@NodeRef)
                     tag:     trapp)
         if TrItem.version >= DBItem.version
            andthen TrItem.secret == DBItem.secret
            andthen ({Not DBItem.locked} orelse ({HasFeature DBItem locker} andthen DBItem.locker==ALockKey)) then
            %LockedItem = {AdjoinAt DBItem locked true}
            %in
            Vote.vote = brewed
            %{@DB put(HKey TrItem.key {AdjoinAt LockedItem locker Tid})}

            %{System.show 'Debug: Putting lock in DB for key '#TrItem.key#' for leader with peer id '#@Leader.ref.id}
            {@DB put(HKey TrItem.key item(key:TrItem.key
                             		secret:TrItem.secret
                             		value:TrItem.value
                             		version:TrItem.version
                             		readers:TrItem.readers  
                             		locked:true
                             		locker:ALockKey))}   % changing Tid due to a bug of name variable
         else
            Vote.vote = denied
         end
         {@MsgLayer dsend(to:@Leader.ref 
                          {Record.adjoinAt Vote tmid @Leader.id})}
         /*for TM in RTMs do
            {@MsgLayer dsend(to:TM.ref {Record.adjoinAt Vote tmid TM.id})}
         end*/
         %{System.show 'Debug:Sent Vote '#Vote.vote#' for key '#TrItem.key#' for leader with peer id '#@Leader.ref.id}
      end

      proc {LockGranted granted}
         {Suicide}
      end

      proc {Abort abort}
	DBItem
        in
        DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
        if DBItem \= NOT_FOUND andthen DBItem.locked andthen 
          {HasFeature DBItem locker} andthen DBItem.locker == NewItem.lockkey then
               %{System.show 'Debug:releasing lock in abort for key '#NewItem.item.key#' for leader with peer id '#@Leader.ref.id}
               {@DB put(NewItem.hkey NewItem.item.key {Record.adjoin DBItem
                                                       item(locked:false
                                                            locker:none)})}
        end
        {Suicide}
      end

       proc {Update update(hkey:HKey item:Item tid:_ lockkey:TheLockKey protocol:_ tag:trapp)}
         DBItem = {@DB get(HKey Item.key $)}
         in
         %{System.show 'Reached update at TP '#@NodeRef.id#' for key '#Item.key#' found DB Item '#DBItem}
         if DBItem \= NOT_FOUND andthen DBItem.locked andthen DBItem.locker==TheLockKey then
                %{System.show 'Debug:Releasing lock and updating for key '#Item.key#' at '#@NodeRef.id}
         	{@DB put(HKey Item.key item(key:Item.key
                                             secret:Item.secret
                                             value:Item.value
                                             version:Item.version+1
                                             readers:Item.readers
                                             locked:false
                                             locker:none))}
	 end
         {Suicide}
      end

      proc {LeaderChanged Event}
         DBItem
         in
         DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
         %{System.show 'Reached Leader Change at TP '#@NodeRef.id#' for key '#NewItem.item.key}
         if DBItem \= NOT_FOUND then
            {@DB put(NewItem.hkey DBItem.key {Record.adjoin DBItem item(locked:false locker:none)})}
         %else
         %   {@DB delete(NewItem.hkey NewItem.item.key)}
         end
         {Suicide} 
      end

      proc {ReleaseLock releaseLock(hkey:HKey item:Item tid:_ lockkey:TheLockKey protocol:_ tag:trapp)}
         DBItem = {@DB get(HKey Item.key $)}
         in
         %{System.show 'Debug: Reached Release lock for key '#Item.key#' at TP '#@NodeRef.id}
         if DBItem \= NOT_FOUND andthen DBItem.locked andthen DBItem.locker==TheLockKey then
         	%{System.show 'Debug: Releasing lock for key '#Item.key#' at TP '#@NodeRef.id}
		{@DB put(HKey Item.key item(key:Item.key
                                             secret:Item.secret
                                             value:Item.value
                                             version:Item.version
                                             readers:Item.readers
                                             locked:false
                                             locker:none))}
         end
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
                     granted:       LockGranted
                     abort:         Abort
                     update:        Update
                     releaseLock:   ReleaseLock
                     leaderChanged: LeaderChanged
                     %newReader:     NewReader
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
      end
      MsgLayer = {NewCell Component.dummy}
      DB   = {NewCell Component.dummy}      

      Id       = {Name.new}
      NodeRef  = {NewCell noref}
      Leader   = {NewCell noleader}

      Self
   end
end  

