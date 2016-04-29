/*-------------------------------------------------------------------------
 *
 * Clansman.oz
 *
 *    Core of Node's implementation given as functor to be imported. To run
 *    this code on its own processor, use Node.oz as ./node
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
 *-------------------------------------------------------------------------
 */

functor
import
   Application
   Connection
   Pickle
   System
   ThePbeer       at '../pbeer/Pbeer.ozf'
   TokenPassing   at 'TokenPassing.ozf'
export
   Run
define

   Say   = System.showInfo

   proc {VerboseLoop Pbeer}
      Pred Succ SelfId
   in
      SelfId = {Pbeer getId($)}
      {Delay 1000}
      Pred = {Pbeer getPred($)}
      Succ = {Pbeer getSucc($)}
      {Say Pred.id#"<--"#SelfId#"-->"#Succ.id}
      {VerboseLoop Pbeer}
   end

   proc {TokenLoop Args PbeerToken}
      Size
   in
      {Delay 2000}
      {PbeerToken ringTrip(Size)}
      {Wait Size}
      if Size == Args.size then
         {Say "\t\t\tWe got the expected size: "#Size}
      else
         {Say "\t\t\tRing size estimation: "#Size}
         {TokenLoop Args PbeerToken}
      end
   end

   proc {ExecLoop Event PbeerToken}
      proc {TriggerEvent Apbeer}
         Flag
      in
         {Apbeer Event(Flag)}
         {Wait Flag}
      end
      RoundFlag
   in
      {Delay 1000}
      {PbeerToken ringTripExec(TriggerEvent RoundFlag)}
      {Wait RoundFlag}
   end

   proc {RefreshFingersLoop PbeerToken}
      {Say "\tRefreshing fingers..."}
      {ExecLoop refreshFingers PbeerToken}
   end

   proc {FindRSetLoop PbeerToken}
      {Say "\tFindRSet triggered..."}
      {ExecLoop findRSet PbeerToken}
   end

   proc {SizeRSetFingers Args PbeerToken}
      %% Measure network size, and notify when Args.size is reached
      {TokenLoop Args PbeerToken}
      %% Prepare infrastructure to run transactions
      {RefreshFingersLoop PbeerToken}
      {FindRSetLoop PbeerToken}
   end

   proc {Run Args}
      PbeerToken
      Pbeer
      Store
      JoinAck
   in
      Pbeer    = {ThePbeer.new args(firstAck:JoinAck)}
      Store    = {Connection.take {Pickle.load Args.store}}

      %% Creating the network is done here.
      %% Register network if master. Join existing network otherwise
      if Args.master then
         {Send Store registerAccessPoint(Args.ring {Pbeer getFullRef($)})}
         {Send Store registerPbeer(Args.ring Pbeer)}
      else
         RingRef
      in
         {Send Store getAccessPoint(Args.ring RingRef)}
         case RingRef
         of none then
            {Say "couldn't get the ref to the ring"}
            {Application.exit 0}
         else
            {Pbeer join(RingRef)}
            {Wait JoinAck}
            {Send Store registerAccessPoint(Args.ring {Pbeer getFullRef($)})}
            {Send Store registerPbeer(Args.ring Pbeer)}
         end
      end

      %% Install the TokenPassing service
      PbeerToken  = {TokenPassing.new args(pbeer:Pbeer say:Say)}
      {Pbeer setListener(PbeerToken)}

      if Args.busy then
         thread
            {VerboseLoop Pbeer}
         end
      end

      if Args.master then
         thread
            {SizeRSetFingers Args PbeerToken}
         end
      end
   end

end
