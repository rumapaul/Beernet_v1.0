/*-------------------------------------------------------------------------
 *
 * Read.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers a transaction.
 *    Transactions can batch several operations into a single transaction, but
 *    in this case, a single read operation is performed.
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

   fun {MakeTransaction Key Value}
      proc {$ TM}
         Value = {TM read(Key $)}
      end
   end

   proc {Run Args}
      Pbeer
      Key
      Outcome
      Trans
   in
      if Args.help then
         {PbeerBaseArgs.helpMessage [key cap ring store protocol] nil read}
         {Application.exit 0}
      end
      Pbeer = {SetsCommon.getPbeer Args.store Args.ring}
      Key   = {SetsCommon.capOrKey Args.cap Args.key}
      Trans = {MakeTransaction Key Outcome}
      {Pbeer runTransaction(Trans _ Args.protocol)}
      {Wait Outcome}
      {System.showInfo Outcome}
      {Application.exit 0}
   end
end
