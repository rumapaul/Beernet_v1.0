/*-------------------------------------------------------------------------
 *
 * DHT.oz
 *
 *    This module provides the basic operations for a distributed hash table:
 *    put, get, delete
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
 *    Last change: $Revision: 418 $ $Author: ruma $
 *
 *    $Date: 2015-09-25 20:43:05 +0200 (Fri, 9 Sept 2015) $
 *
 * NOTES
 *      
 *    The basic operations for distributed hash table do not provide any sort
 *    of replication.  For replicated storage use the transactional layer.
 *
 *    This component needs a messaging layer to be set. It uses SimpleSDB as
 *    default database, it uses a default maxkey but it basically needs one as
 *    argument.
 *
 *    The basic operations provided for key/value pairs are: put(key value
 *    secret) - get(key) - delete(key secret).
 *
 *    SimpleSDB is used to store key/value pairs which are protected with
 *    secrets. The structure to store key/value pairs is the following: There
 *    is a dictionary to associate each key1 with its own dictionary. The
 *    second dictionary associates key2 to each value.
 *
 *    Each of these dictionaries associates a value to its operation id (opid).
 *    The key of the dictionary corresponds to a hash key for the value. The
 *    dictionary value is a tuple containing the stored value together with the
 *    opid.
 *
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../corecomp/Component.ozf'
   Constants   at '../commons/Constants.ozf'
   Utils       at '../utils/Misc.ozf'
export
   New
define

   NO_ACK      = Constants.noAck

   fun {New CallArgs}
      Self
      %Listener
      MsgLayer

      Args
      DB
      DBMan
      Gvars
      Gid
      MaxKey
      NodeRef

      %% === Auxiliar functions =============================================

      fun {NextGid}
         OldGid NewGid
      in
         OldGid = Gid := NewGid
         NewGid = OldGid + 1
         NewGid
      end

      proc {SendNeedItem Key Val Type}
         HKey
         NewGid
      in
         HKey     = {Utils.hash Key @MaxKey}
         NewGid   = {NextGid}
         Gvars.NewGid := data(var:Val type:Type)
         {@MsgLayer send(to:HKey
                         needItem(HKey Key src:@NodeRef gid:NewGid tag:dht))}
      end

      %% --- Handling NeedItem back replies ---------------------------------

      proc {HandleBind ClientVar Val}
         ClientVar = Val
      end

      ValueHandle = handles(pair:HandleBind bind:HandleBind)

      %% === Events =========================================================

      %% --- Key/Value pairs API for applications ---------------------------
      proc {Delete delete(k:Key s:Secret r:Result)}
         HKey NewGid
      in
         HKey = {Utils.hash Key @MaxKey}
         NewGid   = {NextGid}
         Gvars.NewGid := data(var:Result type:bind)
         {@MsgLayer send(deleteItem(hk:HKey
                                    k:Key
                                    s:Secret
                                    src:@NodeRef
                                    gid:NewGid
                                    tag:dht) to:HKey)}
      end
   
      proc {Get get(k:Key v:?Val)}
         {SendNeedItem Key Val pair}
      end

      proc {Put put(s:Secret k:Key v:Val r:Result)}
         HKey NewGid
      in
         HKey     = {Utils.hash Key @MaxKey}
         NewGid   = {NextGid}
         Gvars.NewGid := data(var:Result type:bind)
         {@MsgLayer send(putItem(hk:HKey
                                 k:Key
                                 v:Val
                                 s:Secret
                                 src:@NodeRef
                                 gid:NewGid
                                 tag:dht) to:HKey)}
      end

      %% --- Events used by system protocols --------------------------------

      %% To be used locally, within the peer. (it binds a variable)
      proc {GetItem getItem(HKey Key ?Val)}
         {@DB get(HKey Key Val)}
      end

      proc {DeleteItem Event}
         HKey = Event.hk
         Key = Event.k
         Secret = Event.s
         Gid = Event.gid
         Src = Event.src
         Result
      in
         {@DB delete(HKey Key Secret Result)}
         {@MsgLayer dsend(to:Src bindResult(gid:Gid r:Result tag:dht))}
      end

      proc {NeedItem needItem(HKey Key src:Src gid:AGid tag:dht)}
         Val
      in
         {GetItem getItem(HKey Key Val)}
         {@MsgLayer dsend(to:Src needItemBack(gid:AGid value:Val tag:dht))}
      end

      proc {NeedItemBack needItemBack(gid:AGid value:Val tag:dht)}
         Gdata
      in
         Gdata = {Dictionary.condGet Gvars AGid data(var:_ type:pair)}
         {ValueHandle.(Gdata.type) Gdata.var Val}
         {Dictionary.remove Gvars AGid}
      end

      proc {PutItem Event}
         HKey = Event.hk
         Key = Event.k
         Val = Event.v
         Secret = Event.s
         Gid = Event.gid
         Src = Event.src
         Result
      in
         {@DB put(HKey Key Val Secret Result)}
         if Gid \= NO_ACK then
            {@MsgLayer dsend(to:Src bindResult(gid:Gid r:Result tag:dht))}
         end
      end

      proc {BindResult bindResult(gid:Gid r:Result tag:dht)}
         Gdata
      in
         Gdata = {Dictionary.condGet Gvars Gid data(var:_ type:bind)}
         {ValueHandle.(Gdata.type) Gdata.var Result}
         {Dictionary.remove Gvars Gid}
      end

      %% --- Data Management ------------------------------------------------
      proc {NewPred newPred(old:OldPred new:NewPred tag:data)}
         Entries
      in
         {@DB dumpRange(OldPred.id NewPred.id Entries)}
         if Entries\=nil then
         	{@MsgLayer dsend(to:NewPred insertData(entries:Entries tag:dht))}
	end
      end

      proc {InsertData insertData(entries:Entries tag:dht)}
         if Entries \= nil then
            {@DB insert(Entries _/*Result*/)}
         end
      end

      %% --- Component Setters ----------------------------------------------
      proc {SetDB setDB(ADataBase)}
         @DB := ADataBase
      end

      proc {SetMaxKey setMaxKey(Int)}
         MaxKey := Int
      end

      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
      end

      Events = events(
                     %% Key/Value pairs
                     delete:        Delete
                     get:           Get
                     put:           Put
                     %% System protocols
                     bindResult:    BindResult
                     deleteItem:    DeleteItem
                     getItem:       GetItem
                     needItem:      NeedItem
                     needItemBack:  NeedItemBack
                     putItem:       PutItem
                     %% Setters
                     setDB:         SetDB
                     setMaxKey:     SetMaxKey
                     setMsgLayer:   SetMsgLayer
                     %% Data management
                     newPred:       NewPred
                     insertData:    InsertData
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

      Args     = {Utils.addDefaults CallArgs def(maxKey:Constants.largeKey)}
      DBMan    = Args.dbman
      DB       = {NewCell {DBMan getCreate(name:dht type:secrets db:$)}}
      MaxKey   = {NewCell Args.maxKey}
      Gvars    = {Dictionary.new}
      Gid      = {NewCell 0}
      NodeRef  = {NewCell noref}

      Self
   end

end
