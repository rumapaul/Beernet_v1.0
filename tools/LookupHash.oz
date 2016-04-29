/*-------------------------------------------------------------------------
 *
 * LookupHash.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers a lookup operation
 *    for a hash key. This means that the key is an integer, and the it
 *    bypasses the hash function.
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
         {PbeerBaseArgs.helpMessage [hashkey ring store] nil lookupHash}
         {Application.exit 0}
      end
      Pbeer    = {PbeerCommon.getPbeer Args.store Args.ring}
      Result   = {Pbeer lookupHash(hkey:Args.hashkey res:$)}
      {Wait Result}
      {System.show Result.id}
      {Application.exit 0}
   end
end


