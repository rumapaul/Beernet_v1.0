/*-------------------------------------------------------------------------
 *
 * StopNetwork.oz
 *
 *    It connects to the stop-service port to send the stop message. This
 *    operation will result on killing all oz processes in the list of nodes
 *    given to the cluster mode, so be very carefull about this operation,
 *    because it is too agressive.
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

   STOP_PORT   = {BaseArgs.getDefault achel}

   Say = System.showInfo

   %% ListPbeers uses the following Args from BaseArgs:
   %%    ring
   %%    store
   DefArgs = record(stopport(single   type:int default:STOP_PORT))

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" stop [option]"}
      {Say ""}
      {Say "Options:"}
      {Say '#'("      --stopport\tTicket to stop the network (default: "
               STOP_PORT ")")}
      {Say ""}
   end

   proc {Run Args}
      StopPort StopAck
   in
      if Args.help then
         {HelpMessage}
         {Application.exit 0}
      end

      StopPort = {Connection.take {Pickle.load Args.stopport}}
      {Port.send StopPort stop(StopAck)}
      {Wait StopAck}
      {Say "Everything will perish!"}
      {Application.exit 0}
   end

end
