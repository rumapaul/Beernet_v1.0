/*-------------------------------------------------------------------------
 *
 * ListPbeers.oz
 *
 *    It connects to any pbeer from a given ring, and list pbeers from it
 *    following the successor pointer. It misses branches if any. The amount of
 *    pbeers to be listed is parametrizable.
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
   Property
   Pickle
   System
   BaseArgs at 'BaseArgs.ozf'
export
   DefArgs
   Run
define

   START_KEY   = 0
   MAX         = 50
   STORE_TKET  = {BaseArgs.getDefault store}
   RING_NAME   = {BaseArgs.getDefault ring}

   Say = System.showInfo

   %% ListPbeers uses the following Args from BaseArgs:
   %%    ring
   %%    store
   DefArgs = record(
                     fromkey(single type:int default:START_KEY)
                     max(single     type:int default:MAX)
                   )

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" list [option]"}
      {Say ""}
      {Say "Options:"}
      {Say "  -s, --store\tTicket to the store (default: "#STORE_TKET#")"}
      {Say "  -r, --ring\tRing name (default: "#RING_NAME#")"}
      {Say "      --max\tMaximum pbeers id to display (default: "#MAX#")"}
      {Say '#'("      --fromkey\tStarting point for listing (default: "
               START_KEY ")")}
      {Say ""}
   end

   fun {NewLogger DoneKey}
      LogStream
      proc {LoopLog Msg|MoreMsgs}
         case Msg
         of pid(Pid) then
            {Say Pid}
         [] done(Key) then
            if Key == DoneKey then
               thread
                  {Delay 500}
                  {Say "No more pbeers to show"}
                  {Application.exit 0}
               end
            end
         end
         {LoopLog MoreMsgs}
      end
   in
      thread
         {LoopLog LogStream}
      end
      {Port.new LogStream}
   end

   proc {Run Args}
      Mordor
      Pbeer
      First
      DoneKey
      Logger
      proc {SendId CurrentPbeer}
         PbeerId
      in
         PbeerId = {CurrentPbeer getId($)}
         {Send Logger pid(PbeerId)}
      end
      proc {SendDone}
         {Send Logger done(DoneKey)}
      end
   in
      if Args.help then
         {HelpMessage}
         {Application.exit 0}
      end

      DoneKey  = {Name.new}
      Logger   = {NewLogger DoneKey}
      Mordor   = {Connection.take {Pickle.load Args.store}}
      Pbeer    = {Send Mordor getPbeer(Args.ring $)}
      First    = {Pbeer lookupHash(hkey:Args.fromkey res:$)}
      {Pbeer send(startPassExecCount(SendId SendDone Args.max tag:tokken)
                  to:First.id)}
   end

end
