/*-------------------------------------------------------------------------
 *
 * ValueSet-TP.oz
 *
 *    Transaction Participant for the Key/Value-Set abstraction   
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
 *    Implementation of transaction participant (TP) role on the key/value-set
 *    protocol, which also uses consensus, as in Paxos, but it does not lock
 *    the value-sets.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   System
   Constants      at '../../commons/Constants.ozf'
   Component      at '../../corecomp/Component.ozf'
   HashedList     at '../../utils/HashedList.ozf'
   Utils          at '../../utils/Misc.ozf'
export
   New
define

   BAD_SECRET  = Constants.badSecret
   NO_VALUE    = Constants.noValue
   NO_SECRET   = Constants.noSecret
   SET_MAX_KEY = Constants.largeKey

   fun {New CallArgs}
      Self
      %Listener
      MsgLayer
      NodeRef
      DB

      Id
      NewOp
      Leader
      RTMs

      %% === Auxiliar Functions =============================================
      local
         Ok    = ok(add:1 remove:~1)
         Tmp   = tmp(add:tmp(add:conflict remove:not_found)
                     remove:tmp(add:duplicated remove:conflict))
         Final = ok(0:ok(add:brewed remove:not_found)
                    1:ok(add:duplicated remove:brewed))
         fun {DecisionLoop Ops NewOp State}
            case Ops
            of Op|MoreOps then
               if Op.val == NewOp.val then
                  if Op.sval == NewOp.sval then
                     if Op.status == tmp then
                        Tmp.(Op.op).(NewOp.op)
                     else
                        {DecisionLoop MoreOps NewOp (State + Ok.(Op.op))}
                     end
                  else
                     BAD_SECRET
                  end
               else
                  {DecisionLoop MoreOps NewOp State}
               end
            [] nil then
               Final.State.(NewOp.op)
            end
         end
      in
         fun {DecideVote Set NewOp}
            if Set == NO_VALUE then
               if NewOp.op == createSet then
                  brewed
               elseif NewOp.op == destroySet then
                  not_found
               else
                  Final.0.(NewOp.op)
               end
            else
               if {List.member NewOp.op [createSet destroySet]} then
                  if NewOp.msec == Set.msec then
                     brewed
                  else
                     {System.show 'got a bad secret...'}
                     BAD_SECRET
                  end
               elseif Set.sec == NewOp.sec then
                  {DecisionLoop {HashedList.getValues Set.ops} NewOp 0}
               else
                  BAD_SECRET
               end
            end
         end
      end

      %% WARNING: Creation of set with silent failure
      proc {CreateSet HKey Key Secret MasterSecret}
         if {@DB get(HKey Key $)} == NO_VALUE then
            {@DB put(HKey
                     Key
                     set(sec:Secret msec:MasterSecret ops:nil)
                     MasterSecret
                     _)}
         end  %% TODO: Return a reasonable error 
      end

      %% WARNING: Destruction of set with silent failure
      proc {DestroySet HKey Key MasterSecret}
         R
      in
         {System.showInfo "Going to destroy set under key "#Key}
         {@DB delete(HKey Key MasterSecret R)}
         {Wait R}
         {System.show R}
      end

      proc {AddToSet HKey Key Secret Val}
         Set
         HVal
      in
         Set   = {@DB get(HKey Key $)}
         HVal  = {Utils.hash Val SET_MAX_KEY}
         if Set \= NO_VALUE then
            if Set.sec == Secret then
               {@DB put(HKey 
                        Key
                        set(ops:{HashedList.add Set.ops Val HVal}
                            sec:Set.sec
                            msec:Set.msec)
                        Set.msec
                        _)}
            end
         else
            {CreateSet HKey Key Secret NO_SECRET}
            {AddToSet HKey Key Secret Val}
         end
      end

      proc {RemoveFromSet HKey Key Secret Val}
         Set
         HVal
      in
         Set   = {@DB get(HKey Key $)}
         HVal  = {Utils.hash Val SET_MAX_KEY}
         if Set \= NO_VALUE andthen Set.sec == Secret then
            {@DB put(HKey
                     Key
                     sets(ops:{HashedList.remove Set.ops Val HVal}
                          sec:Set.sec
                          msec:Set.msec)
                     Set.msec
                     _)}
         end
      end

      %% === Events =========================================================

      %% --- Interaction with TPs ---
      proc {Brew brew(hkey:   HKey
                      leader: TheLeader
                      rtms:   TheRTMs
                      tid:    Tid
                      item:   Item 
                      protocol:_ 
                      tag:trapp)}
         DBSet
         Vote
         Key
      in 
         RTMs     = TheRTMs
         NewOp    = {Record.adjoinAt Item hkey HKey}
         Leader   := TheLeader
         Key      = Item.key
         DBSet    = {@DB get(HKey Key $)}
         %% Brewing vote - tmid needs to be added before sending
         Vote = vote(vote:    {DecideVote DBSet NewOp}
                     key:     Key 
                     version: 0
                     tid:     Tid 
                     tp:      tp(id:Id ref:@NodeRef)
                     tag:     trapp)
         if Vote.vote == brewed then
            if NewOp.op == createSet then
               {CreateSet HKey Key NewOp.sec NewOp.msec}
            elseif NewOp.op == destroySet then
               skip
            else
               {AddToSet HKey Key NewOp.sec {Record.adjoinAt NewOp status tmp}}
            end
         end
         {@MsgLayer dsend(to:@Leader.ref 
                          {Record.adjoinAt Vote tmid @Leader.id})}
         for TM in RTMs do
            {@MsgLayer dsend(to:TM.ref {Record.adjoinAt Vote tmid TM.id})}
         end
      end

      proc {RemoveNewOp}
         {RemoveFromSet NewOp.hkey
                        NewOp.key
                        NewOp.sec
                        {Record.adjoinAt NewOp status tmp}}
      end

      proc {Abort abort}
         if NewOp.op == createSet then
            %% There might be a race condition between adding a operation and
            %% creating a set.
            {DestroySet NewOp.hkey NewOp.key NewOp.msec}
         elseif NewOp.op == destroySet then
            skip
         else
            {RemoveNewOp}
         end
      end

      proc {Commit commit}
         {System.showInfo "Got commit... going to "#NewOp.op#" for "#NewOp.key}
         if NewOp.op == createSet then
            skip
         elseif NewOp.op == destroySet then
            {DestroySet NewOp.hkey NewOp.key NewOp.msec}
         else 
            {RemoveNewOp}
            {AddToSet NewOp.hkey
                      NewOp.key
                      NewOp.sec
                      op(id:NewOp.id
                         op:NewOp.op
                         val:NewOp.val
                         sval:NewOp.sval
                         status:ok)}
         end
      end

      %% --- Various --------------------------------------------------------

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
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         %Listener = FullComponent.listener
      end
      MsgLayer = {NewCell Component.dummy}
      DB       = {NewCell Component.dummy}      

      Id       = {Name.new}
      NodeRef  = {NewCell noref}
      Leader   = {NewCell noleader}

      Self
   end
end  

