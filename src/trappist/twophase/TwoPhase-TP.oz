/*-------------------------------------------------------------------------
 *
 * TwoPhase-TP.oz
 *
 *    Transaction Participant for the Two-Phase Commit Protocol    
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: boriss $
 *
 *    $Date: 2011-05-19 21:45:21 +0200 (Thu, 19 May 2011) $
 *
 * NOTES
 *
 *    Implementation of the classical two-phase commit protocol for replicated
 *    databases. This is one of the replicas of the protocol. Known as the
 *    transaction participant.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Component      at '../../corecomp/Component.ozf'
export
   New
define

   fun {New CallArgs}
      Self
      MsgLayer
      DB

      Id
      NodeRef
      NewItem
      Leader
      %% --- Event --

      %% --- Interaction with TPs ---
      proc {Brew brew(hkey:   HKey
                      tm:     TM 
                      tid:    Tid
                      tmid:   TMid
                      item:   TrItem 
                      protocol:_ 
                      tag:trapp)}
         Tmp
         DBItem
         Vote
      in 
         NewItem  = item(hkey:HKey item:TrItem tid:Tid tmid:TMid)
         Leader   = TM
         Tmp      = {@DB get(HKey TrItem.key $)}
         DBItem   = if Tmp == 'NOT_FOUND' then
                        item(key:      TrItem.key
                             value:    Tmp 
                             version:  0
                             readers:  nil
                             locked:   false)
                    else
                       Tmp
                    end
         %% Brewing vote
         Vote = vote(vote:    _
                     key:     TrItem.key 
                     version: DBItem.version 
                     tid:     Tid 
                     tmid:    TMid
                     tp:      tp(id:Id ref:@NodeRef)
                     tag:     trapp)
         if TrItem.version >= DBItem.version andthen {Not DBItem.locked} then
            Vote.vote = brewed
            {@DB put(HKey TrItem.key {AdjoinAt DBItem locked true})}
         else
            Vote.vote = denied
         end
         {@MsgLayer dsend(to:Leader Vote)}
      end

      proc {PutItemAndAck HKey Key Item}
         {@DB  put(HKey Key {Record.adjoinAt Item locked false})}
         {@MsgLayer dsend(to:Leader ack(key: Key
                                        tid: NewItem.tid
                                        tmid:NewItem.tmid
                                        tp:  tp(id:Id ref:@NodeRef)
                                        tag: trapp))}
      end

      proc {Abort abort}
         DBItem
      in
         DBItem = {@DB get(NewItem.hkey NewItem.item.key $)}
         {PutItemAndAck NewItem.hkey NewItem.item.key DBItem}
      end

      proc {Commit commit}
         {PutItemAndAck NewItem.hkey NewItem.item.key NewItem.item}
      end

      %% --- Various ---

      proc {GetId getId(I)}
         I = Id
      end

      proc {SetDB setDB(NewDB)}
         DB := NewDB
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
                     %% Various
                     getId:         GetId
                     setDB:         SetDB
                     setMsgLayer:   SetMsgLayer
                     )
   in
      Self     = {Component.new Events}.trigger
      MsgLayer = {NewCell Component.dummy}
      DB       = {NewCell Component.dummy}      

      NodeRef  = {NewCell noref}
      Id       = {Name.new}

      Self
   end
end  

