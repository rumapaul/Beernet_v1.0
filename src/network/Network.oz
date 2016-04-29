/*-------------------------------------------------------------------------
 *
 * Network.oz
 *
 *    Comunication layer. Uses Pbeer-point-to-point and a failure detector
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
 *    Last change: $Revision: 410 $ $Author: ruma $
 *
 *    $Date: 2016-04-10 01:05:05 +0200 (Sun, 10 April 2016) $
 *
 * NOTES
 *      
 *    Implementation of the component that provides high level events to
 *    comunicate with other nodes on the network. It uses the Pbeer
 *    point-to-point link (Pbeerp2p) to send and deliver messages. It uses a
 *    failure detector to monitor the pbeers registered with 'monitor'.
 *
 * EVENTS
 *
 *    Accepts: sendTo(Dest Msg) - Sends message Msg to Node Dest. Dest is a
 *    record of the form node(id:Id port:P), where Id is the identifier
 *    equivalent to self identifier, and P is an oz port. Msg can be anything.
 * 
 *    Accepts: getPort(P) - Binds P to the port of this site. It is a way of
 *    building a self reference to give to others.
 *
 *    Accepts: monitor(P) - Register P on the failure detector to be constantly
 *    monitored.
 *
 *    Indication: It triggers whatever message is delivered by pp2p link as an
 *    event on the listener.
 *
 *    Indication: crash(P) and alive(P) - Events coming from the failure
 *    detector.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Board             at '../corecomp/Board.ozf'
   Component         at '../corecomp/Component.ozf'
   Pbeerp2p          at 'Pbeerp2p.ozf'
   FailureDetector   at 'FailureDetector.ozf'
  
export
   New
define

   fun {New}
      ComLayer       % Pbeer level communication layer
      FailDetector   % Failure Detector
      Listener       % Component where the deliver messages will be triggered
      Self           % Full Component
      Suicide        % Stop reciving message => Kill itself

      %%--- Events ---

      proc {Any Event}
         {@Listener Event}
      end

      proc {SetId setId(NewId)}
         {ComLayer setId(NewId)}
         {FailDetector setPbeer({ComLayer getRef($)})}
      end

      proc {SignalDestroy signalDestroy}
         {ComLayer signalDestroy}
         {FailDetector signalDestroy}
         {Suicide}
      end

      Events = events(
                  any:           Any
                  getPort:       ComLayer
                  getRef:        ComLayer
                  monitor:       FailDetector
                  pp2pDeliver:   ComLayer
                  sendTo:        ComLayer
                  setId:         SetId
                  setLogger:     ComLayer
                  signalDestroy: SignalDestroy
                  setFDParams: 	 FailDetector
	          setPriority: 	 FailDetector
                  stopMonitor:   FailDetector
		  signalALinkFailure: ComLayer 
                  signalALinkRestore: ComLayer
                  injectLinkDelay: ComLayer
                  injectLowLinkDelay: ComLayer
                  injectNoLinkDelay:  ComLayer
                  simulateALinkDelay: ComLayer
                  injectDelayVariance: ComLayer
                  )
   in
      ComLayer       = {Pbeerp2p.new}
      FailDetector   = {FailureDetector.new}
      Self           = {Component.new Events}
      Listener       = Self.listener
      Suicide        = Self.killer
      {FailDetector setComLayer(ComLayer)}
      local
         ThisBoard Subscriber
      in
         [ThisBoard Subscriber] = {Board.new}
         {Subscriber Self.trigger}
         {Subscriber tagged(FailDetector fd)}
         {ComLayer setListener(ThisBoard)}
      end
      {FailDetector setListener(Self.trigger)}
      Self.trigger
   end
end
