/*-------------------------------------------------------------------------
 *
 * Pbeer.oz
 *
 *    Global API to create a Beernet Peer from an Oz program.
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
 *    Last change: $Revision: 430 $ $Author: ruma $
 *
 *    $Date: 2016-04-21 19:03:14 +0200 (Thurs, 21 April 2016) $
 *
 * NOTES
 *      
 *    This is the highest level component of Beernet. It implements the events
 *    described on the general API connecting with the other components that
 *    implements each behaviour.
 *
 *-------------------------------------------------------------------------
 */

functor
import
   System
   Board          at '../corecomp/Board.ozf'
   Component      at '../corecomp/Component.ozf'
   Constants      at '../commons/Constants.ozf'
   DBManager      at '../database/DBManager.ozf'
   RelaxedRing    at '../son/relaxed-ring/Node.ozf'
   Replication    at '../trappist/SymmetricReplication.ozf'
   TheDHT         at '../dht/DHT.ozf'
   TheMsgLayer    at '../messaging/MsgLayer.ozf'
   TransLayer     at '../trappist/Trappist.ozf'
   Utils          at '../utils/Misc.ozf'
export
   New
define

   NO_SECRET   = Constants.public
   REPL_FACT   = Constants.pbeerReplicationFactor

   Say = System.showInfo

   fun {New Args}
      Listener % Component's listener
      Node     % Node implementing the behaviour
      DBMan    % Database manager
      DHT      % DHT functionality
      MsgLayer % Reliable messaging layer
      Replica  % Symmetric replication
      Self     % This component
      Trappist % Transactional layer

      %% Inbox for receiving messages
      Inbox    % Port to receive messages
      NewMsgs  % Dynamic head of new messages

      %%--- Events ---

      proc {Any Event}
         %% Messages comming from the MsgLayer
         %% Mainly used by the application.
         {@Listener Event}
         {Port.send Inbox Event}
      end

      proc {ClientEvents Event}
         {Port.send Inbox Event.msg}
      end

      proc {Broadcast Event}
%         broadcast(Range Msg) = Event
%      in
         skip
      end
      
      proc {InjectPermFail injectPermFail}
         {ClientEvents clientEvents(msg:suicide)}
         {@Node injectPermFail}
         {@Trappist signalDestroy}
         {@Replica signalDestroy}
         {@DHT signalDestroy}
         {@DBMan signalDestroy}
         {@MsgLayer signalDestroy}
      end
      
      proc {Join join(RingRef)}
         {@Node startJoin(succ:RingRef.pbeer ring:RingRef.ring)} 
      end
      
      proc {Leave Event}
         leave = Event
      in
         {@Node injectPermFail}
      end
      
      proc {ReceiveTagged receive(Msg)}
         thread
            OldHead NewHead
         in
            OldHead = NewMsgs := NewHead
            case OldHead
            of NewMsg|MoreMsgs then
               Msg = NewMsg
               NewHead = MoreMsgs
            [] nil then
               skip
            end
         end
      end

      proc {SendTagged Event}
      %   send(Msg to:Target ...) = Event
      %in
         {@MsgLayer Event}
      end
    
      %% --- Masking DHT operations put/get/delete for value type mixing ----
      proc {PrePut Event}
         case Event
         of put(Key Val) then
            {@DHT put(s:NO_SECRET k:Key v:Val r:_)}
         [] put(k:Key v:Val r:Result) then
            {@DHT put(s:NO_SECRET k:Key v:Val r:Result)}
         [] put(s:Secret k:Key v:Val r:Result) then
            {@DHT put(s:Secret k:Key v:Val r:Result)}
         else
            raise
               error(wrong_invocation(event:put
                                      found:Event
                                      mustbe:put(s:secret
                                                 k:key
                                                 v:value
                                                 r:result)))
            end
         end
      end

      proc {PreGet Event}
         case Event
         of get(Key Result) then
            {@DHT get(k:Key v:Result)}
         [] get(k:Key v:Result) then
            {@DHT get(k:Key v:Result)}
         [] get(s:_ k:Key v:Result) then
            {Say "DHT Warning: secrets are not used for reading"}
            {@DHT get(k:Key v:Result)}
         else
            raise
               error(wrong_invocation(event:get
                                      found:Event
                                      mustbe:get(k:key v:result)))
            end
         end
      end

      proc {PreDelete Event}
         case Event
         of delete(Key) then
            {@DHT delete(s:NO_SECRET k:Key r:_)}
         [] delete(k:Key r:Result) then
            {@DHT delete(s:NO_SECRET k:Key r:Result)}
         [] delete(s:Secret k:Key r:Result) then
            {@DHT delete(s:Secret k:Key r:Result)}
         else
            raise
               error(wrong_invocation(event:delete
                                      found:Event
                                      mustbe:delete(s:secret
                                                    k:key
                                                    r:result)))
            end
         end
         skip
      end
      %% --- End of Masking DHT operations ----------------------------------

      %% --- Forwarding to DHT with different event name --------------------
      proc {SingleAdd Event}
         {@DHT {Record.adjoinList add {Record.toListInd Event}}}
      end

      proc {SingleRemove Event}
         {@DHT {Record.adjoinList remove {Record.toListInd Event}}}
      end

      proc {SingleReadSet Event}
         {@DHT {Record.adjoinList readSet {Record.toListInd Event}}}
      end
      %% --- end forward to DHT with different event name -------------------

      %% --- Masking Value-Sets operations ----------------------------------
      proc {PreCreate Event}
         case Event
         of createSet(Key) then
            {ToTrappist createSet(k:Key
                                  ms:NO_SECRET
                                  s:NO_SECRET
                                  c:{Port.new _})}
         [] createSet(k:_ ms:_ s:_ c:_) then
            {ToTrappist Event}
         else
            raise
               error(wrong_invocation(event:createSet
                                      found:Event
                                      mustbe:createSet(k:key
                                                       ms:mastersecret
                                                       s:updatesecret
                                                       c:client)))
            end
         end
      end

      proc {PreDestroy Event}
         case Event
         of destroySet(Key) then
            {ToTrappist destroySet(k:Key ms:NO_SECRET c:{Port.new _})}
         [] destroySet(k:_ ms:_ c:_) then
            {ToTrappist Event}
         else
            raise
               error(wrong_invocation(event:destroySet
                                      found:Event
                                      mustbe:destroySet(k:key
                                                        ms:mastersecret
                                                        c:client)))
            end
         end
      end

      proc {PreAdd Event}
         {PreAddRemove Event add}
      end

      proc {PreRemove Event}
         {PreAddRemove Event remove}
      end

      proc {PreAddRemove Event Op}
         case Event
         of Op(Key Val) then
            {ToTrappist Op(k:Key s:NO_SECRET v:Val sv:NO_SECRET c:{Port.new _})}
         [] Op(Key Val Client) then
            {ToTrappist Op(k:Key s:NO_SECRET v:Val sv:NO_SECRET c:Client)}
         [] Op(k:Key v:Val sv:SecVal c:Client) then
            {ToTrappist Op(k:Key s:NO_SECRET v:Val sv:SecVal c:Client)}
         [] Op(k:_ s:_ v:_ sv:_ c:_) then
            {ToTrappist Event}
         else
            raise
               error(wrong_invocation(event:Op
                                      found:Event
                                      mustbe:Op(s:secret
                                                k:key
                                                vs:valuesecret
                                                v:value
                                                c:client)))
            end
         end
      end

      proc {PreReadSet Event}
         case Event
         of readSet(Key Val) then
            {ToTrappist readSet(k:Key v:Val)}
         [] readSet(k:_ v:_) then
            {ToTrappist Event}
         else
            raise
               error(wrong_invocation(event:readSet
                                      found:Event
                                      mustbe:readSet(k:key
                                                     v:value)))
            end
         end
      end

      %% --- End of Masking value-sets operations ---------------------------

      ToNode      = {Utils.delegatesTo Node}
      ToReplica   = {Utils.delegatesTo Replica}
      ToTrappist  = {Utils.delegatesTo Trappist}

      Events = events(
                     any:              Any
                     clientEvents:     ClientEvents
                     broadcast:        Broadcast
                     dsend:            SendTagged
                     getFullRef:       ToNode
                     getId:            ToNode
                     getMaxKey:        ToNode
                     getPred:          ToNode
                     getRange:         ToNode
                     getRef:           ToNode
                     getRingRef:       ToNode
                     getSucc:          ToNode
                     getKnowledge:     ToNode
                     getIdKnowledge:   ToNode
		     getPhase:	       ToNode	
                     setPhaseNotify:   ToNode
                     stopPhaseNotify:  ToNode
                     getBranchSz:      ToNode
                     injectPermFail:   InjectPermFail
		     signalALinkFailure:   ToNode	%R
                     injectLinkDelay:     ToNode
                     injectDelayVariance:  ToNode
                     signalALinkRestore:   ToNode
                     simulateALinkDelay:   ToNode
                     monitor:		   ToNode
                     stopMonitor:	   ToNode
                     introduce:            ToNode
		     startPS:              ToNode
                     startMerger:          ToNode
                     startKB:              ToNode
                     stopPS:               ToNode
                     stopMerger:           ToNode
                     stopKB:               ToNode
                     join:             Join
                     leave:            Leave
                     lookup:           ToNode
                     lookupHash:       ToNode
                     injectNoLinkDelay:      ToNode
                     receive:          ReceiveTagged
                     injectLowLinkDelay:  ToNode
                     refreshFingers:   ToNode
                     send:             SendTagged
                     setLogger:        ToNode
                     %% DHT events
                     delete:           PreDelete
                     get:              PreGet
                     put:              PrePut
                     singleAdd:        SingleAdd
                     singleRemove:     SingleRemove
                     singleReadSet:    SingleReadSet
                     %% Replication events
                     bulk:             ToReplica
                     findRSet:         ToReplica
                     getOne:           ToReplica
                     getAll:           ToReplica
                     getMajority:      ToReplica
                     %% Trappist Transactional layer
                     useSuccBackUp:    ToTrappist
                     becomeReader:     ToTrappist
                     executeTransaction:ToTrappist
                     getLocks:         ToTrappist
                     commitTransaction: ToTrappist
                     abortTransaction:  ToTrappist
                     runTransaction:   ToTrappist
                     add:              PreAdd
                     remove:           PreRemove
                     readSet:          PreReadSet
                     createSet:        PreCreate
                     destroySet:       PreDestroy
                     )

   in
      %% Creating the component and collaborators
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Listener = FullComponent.listener
      end
      Node     = {NewCell {RelaxedRing.new Args}}
      MsgLayer = {NewCell {TheMsgLayer.new args}}
      DBMan    = {NewCell {DBManager.new}}
      local
         MaxKey
      in
         MaxKey   = {@Node getMaxKey($)}
         DHT      = {NewCell {TheDHT.new args(maxKey:MaxKey dbman:@DBMan)}}
         Trappist = {NewCell {TransLayer.new args(maxKey:MaxKey dbman:@DBMan)}}
         Replica  = {NewCell {Replication.new args(maxKey:MaxKey
                                                   repFactor:REPL_FACT
                                                   dbman:@DBMan)}}
      end
      {@MsgLayer setNode(@Node)}
      {@Node setListener(@MsgLayer)}
      {@DHT setMsgLayer(@MsgLayer)}
      {@Replica setMsgLayer(@MsgLayer)}
      {@Trappist setMsgLayer(@MsgLayer)}
      {@Trappist setReplica(@Replica)}
      {@Trappist setListener(Self)}
      local
         StorageBoard StorageSubscriber
      in
         [StorageBoard StorageSubscriber] = {Board.new}
         {StorageSubscriber Self}
         {StorageSubscriber tagged(@DHT dht)}
         {StorageSubscriber tagged(@Replica symrep)}
         {StorageSubscriber tagged(@Trappist trapp)}
         {StorageSubscriber tagged(@DHT data)}
         {StorageSubscriber tagged(@Trappist data)}
         {@MsgLayer setListener(StorageBoard)}
      end

      %% Creating the Inbox abstraction
      local Str in
         Inbox = {Port.new Str}
         NewMsgs = {NewCell Str}
      end

      Self
   end

end

