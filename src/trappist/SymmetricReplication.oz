/*-------------------------------------------------------------------------
 *
 * SymmetricReplication.oz
 *
 *    This module provides operations for symmetric replication on circular
 *    address spaces.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 * 	      Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 480 $ $Author: ruma $
 *
 *    $Date: 2016-04-10 01:21:15 +0200 (Sun, 10 April 2016) $
 *
 * NOTES
 *    
 *    Pre-condition: It needs a messaging layer, a Database manager, and the
 *    Node Reference
 *
 *    Bulk: bulk operations send a message to all peers in the replica set.
 *    
 *    Get One: it bulks a read message, and returns the first answer.
 *
 *    Read All: Returns a list of items from all participants
 *
 *    Read Majority: Returns a list of items from a mojority
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../corecomp/Component.ozf'
   Constants   at '../commons/Constants.ozf'
   Timer       at '../timer/Timer.ozf'
   Utils       at '../utils/Misc.ozf'
   PbeerList   at '../utils/PbeerList.ozf'
export
   New
define
  
   %% Returns a list of 'f' hash keys symmetrically replicated whithin the
   %address space, from 0 to Max. 'f' is the replication Factor. The list
   %starts with the input Key. Note that Key is a key in the hash table. 
   fun {MakeSymReplicas HKey Max Factor}
      Increment = Max div Factor
      fun {GetLoop Iter Last}
         if Iter > 0 then
            New = ((Last + Increment) mod Max)
         in
            New|{GetLoop Iter - 1 New}
         else
            nil
         end
      end
   in
      HKey|{GetLoop Factor-1 HKey}
   end

   fun {New CallArgs}
      Self
      %Listener
      MsgLayer
      NodeRef
      TheTimer

      Args
      DBMan    % Database Manager
      MaxKey   % Maximum key                       
      Factor   % Replication factor
      Gvars                                        
      Gid                                          
      RSet     % ReplicaSet
      RSetOK   % True if RSet is ready to be used
      RSetFlag % To acknowledge when RSet is ready
      Timeout

      fun {NextGid}
         OldGid NewGid
      in
         OldGid = Gid := NewGid
         NewGid = OldGid + 1
         NewGid
      end

      proc {RegisterRead Key Val Type DBid}
         NewGid
      in
         NewGid   = {NextGid}
         Gvars.NewGid := data(var:Val tries:0 state:waiting type:Type)
         {Bulk bulk(to:{Utils.hash Key @MaxKey}
                    read(Key id:NewGid src:@NodeRef db:DBid tag:symrep))}
         {TheTimer startTrigger(@Timeout timeout(NewGid) Self)}
      end

      proc {HandleOne AGid Val Gvar}
         Tries = Gvar.tries+1
      in
         if Val \= 'NOT_FOUND' orelse Tries == @Factor then
            Gvar.var = Val
            {Dictionary.remove Gvars AGid}
         else
            Gvars.AGid := {Record.adjoinAt Gvar tries Tries}
         end
      end

      proc {HandleList AGid Val Gvar Max}
         Tries = Gvar.tries+1
      in
         if Tries == Max then
            if Val == 'NOT_FOUND' then
               Gvar.var = nil
            else
               Gvar.var = Val|nil
            end
            {Dictionary.remove Gvars AGid}
         else
            if Val == 'NOT_FOUND' then
               Gvars.AGid := {Record.adjoin Gvar data(tries:Tries)}
            else
               NewTail
            in
               Gvar.var = Val|NewTail
               Gvars.AGid := {Record.adjoin Gvar data(tries:Tries var:NewTail)}
            end
         end
      end

      proc {HandleAll AGid Val Gvar}
         {HandleList AGid Val Gvar @Factor}
      end

      proc {HandleMajor AGid Val Gvar}
         {HandleList AGid Val Gvar (@Factor div 2 + 1)}
      end

      ReadHandles = handles(first:  HandleOne
                            all:    HandleAll
                            major:  HandleMajor)

      %% --- Events ---

      proc {QuickBulk quickBulk(Msg to:Key)}
         DoBulk
      in
         if @RSetOK then 
            DoBulk = BulkToRSet
         else
            DoBulk = Bulk
         end
         {DoBulk bulk(Msg to:Key)}
      end

      proc {ReverseBulk reverseBulk(Msg to:Key)}
         RepKeys
      in
         RepKeys = {MakeSymReplicas Key @MaxKey @Factor}.2
         for K in RepKeys do
            {@MsgLayer send({Record.adjoinAt Msg hkey K} to:K)}
         end
      end  

      %% Optimization to bulk to a more stable replica set
      proc {BulkToRSet bulk(Msg to:_/*Key*/)}
         for pbeer(id:RepId port:RepPort hkey:HKey) in @RSet do
            {@MsgLayer dsend(to:pbeer(id:RepId port:RepPort)
                             {Record.adjoinAt Msg hkey HKey})}
         end
      end

      %% Bulk message using overlay's routing (logarithmic)
      proc {Bulk bulk(Msg to:HKey)}
         RepKeys
      in
         RepKeys = {MakeSymReplicas HKey @MaxKey @Factor}
         for K in RepKeys do
            {@MsgLayer send({Record.adjoinAt Msg hkey K} to:K)}
         end
      end

      proc {FindRSet findRSet(Flag)}
         {Bulk bulk(to:@NodeRef.id giveMeYourRef(src:@NodeRef tag:symrep))}
         RSet     := {PbeerList.new}
         RSetOK   := false
         @RSetFlag = Flag
      end
      
      proc {GetFactor getFactor(F)}
         F = @Factor
      end

      proc {GetReplicaKeys Event}
         getReplicaKeys(HKey Keys ...) = Event
         MKey
         F
      in
         MKey = if {HasFeature Event maxKey} then Event.maxKey else @MaxKey end
         F    = if {HasFeature Event factor} then Event.factor else @Factor end
         Keys = {MakeSymReplicas HKey MKey F}
      end

      proc {GetReverseKeys Event}
         HKey = Event.1
	 Keys = Event.2
         MKey
         F
      in
         MKey = if {HasFeature Event maxKey} then Event.maxKey else @MaxKey end
         F    = if {HasFeature Event factor} then Event.factor else @Factor end
         %% Removing the original HKey
         Keys = {MakeSymReplicas HKey MKey F}.2
      end

      proc {GetOne getOne(Key ?Val DBid)}
         {RegisterRead Key Val first DBid}
      end

      proc {GetAll getAll(Key ?Vals DBid)}
         {RegisterRead Key Vals all DBid}
      end

      proc {GetMajority getMajority(Key ?Vals DBid)}
         {RegisterRead Key Vals major DBid}
      end

      proc {GiveMeYourRef giveMeYourRef(hkey:HKey src:Src tag:symrep)}
         {@MsgLayer dsend(to:Src myRef(ref:@NodeRef hkey:HKey tag:symrep))}
      end

      proc {MyRef myRef(ref:Pbeer hkey:HKey tag:symrep)}
         RSet := {PbeerList.add {Record.adjoinAt Pbeer hkey HKey} @RSet}
         if {List.length @RSet} == @Factor then
            RSetOK      := true
            @RSetFlag   = unit
            RSetFlag    := _
         end
      end

      proc {Read read(Key id:Gid src:Src hkey:HKey db:DBid tag:symrep)}
         DB Val
      in
         DB = {@DBMan get(name:DBid db:$)}
         {DB get(HKey Key Val)}
         {@MsgLayer dsend(to:Src readBack(value:Val gid:Gid tag:symrep))}
         {Wait Val}
      end

      proc {ReadBack readBack(gid:AGid value:Val tag:symrep)}
         Gvar
      in
         Gvar = {Dictionary.condGet Gvars AGid var(state:gone)}
         if Gvar.state == waiting then
            {ReadHandles.(Gvar.type) AGid Val Gvar}
         end
      end

      proc {SetDBMan setDBMan(NewDBMan)}
         DBMan := NewDBMan
      end

      proc {SetFactor setFactor(F)}
         Factor := F
      end

      proc {SetMaxKey setMaxKey(Key)}
         MaxKey := Key
      end

      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
         RSet     := {PbeerList.new}
         RSetOK   := false
      end

      proc {SetTimeout setTimeout(ATime)}
         Timeout := ATime
      end

      proc {TimeoutEvent timeout(AGid)}
         Gvar
      in
         Gvar = {Dictionary.condGet Gvars AGid var(var:_)}
         Gvar.var = 'NOT_FOUND'
         {Dictionary.remove Gvars AGid}
      end

      Events = events(
                     bulk:          Bulk
                     findRSet:      FindRSet
                     getOne:        GetOne
                     getAll:        GetAll
                     getFactor:     GetFactor
                     getMajority:   GetMajority
                     getReplicaKeys:GetReplicaKeys
                     getReverseKeys:GetReverseKeys
                     giveMeYourRef: GiveMeYourRef
                     myRef:         MyRef
                     quickBulk:     QuickBulk
		     reverseBulk:   ReverseBulk
                     read:          Read
                     readBack:      ReadBack
                     setDBMan:      SetDBMan
                     setFactor:     SetFactor
                     setMaxKey:     SetMaxKey
                     setMsgLayer:   SetMsgLayer
                     setTimeout:    SetTimeout
                     timeout:       TimeoutEvent
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
      TheTimer = {Timer.new}

      Args     = {Utils.addDefaults CallArgs def(maxKey:    Constants.largeKey
                                                 repFactor: Constants.pbeerReplicationFactor
                                                 timeout:   Constants.symmetricReplicationTimeout)}
      MaxKey   = {NewCell Args.maxKey}
      Factor   = {NewCell Args.repFactor}
      Timeout  = {NewCell Args.timeout}
      DBMan    = {NewCell Args.dbman}

      Gvars    = {Dictionary.new}
      Gid      = {NewCell 0}
      NodeRef  = {NewCell noref}
      RSet     = {NewCell {PbeerList.new}}
      RSetOK   = {NewCell false}
      RSetFlag = {NewCell _}

      Self 
   end
   
end
