/*-------------------------------------------------------------------------
 *
 * Erase.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers a transaction.
 *    Transactions can batch several operations into a single transaction, but
 *    in this case, a single erase operation is performed.
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
   System
   PbeerBaseArgs  at 'PbeerBaseArgs.ozf'
   SetsCommon     at 'SetsCommon.ozf'
export
   DefArgs
   Run
define
   DefArgs = nil

   fun {MakeTransaction Key Secret}
      proc {$ TM}
         {TM erase(k:Key s:Secret r:_)}
         {TM commit}
      end
   end

   proc {PrettyPrint Msg}
      if {VirtualString.is Msg} then
         {System.showInfo Msg}
      else
         {System.show Msg}
      end
   end

   proc {Run Args}
      Pbeer
      Key
      MyPort
      Outcome
      Trans
   in
      if Args.help then
         {PbeerBaseArgs.helpMessage [key cap ring secret store protocol]
                                    nil 
                                    erase}
         {Application.exit 0}
      end
      Pbeer = {SetsCommon.getPbeer Args.store Args.ring}
      MyPort= {Port.new Outcome}
      Key   = {SetsCommon.capOrKey Args.cap Args.key}
      Trans = {MakeTransaction Key Args.secret}
      {Pbeer runTransaction(Trans MyPort Args.protocol)}
      {PrettyPrint Outcome.1}
      {Application.exit 0}
   end
end
