/*-------------------------------------------------------------------------
 *
 * LookupHash.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers a lookup operation
 *    for a key. The result is the peer responsible for the hash key of hey.
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
   PbeerCommon    at 'PbeerCommon.ozf'
export
   DefArgs
   Run
define
   DefArgs = nil

   proc {Run Args}
      Pbeer Result
   in
      if Args.help then
         {PbeerBaseArgs.helpMessage [key ring store] nil lookup}
         {Application.exit 0}
      end
      Pbeer    = {PbeerCommon.getPbeer Args.store Args.ring}
      Result   = {Pbeer lookup(key:Args.key res:$)}
      {Wait Result}
      {System.show Result.id}
      {Application.exit 0}
   end
end


