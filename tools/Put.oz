/*-------------------------------------------------------------------------
 *
 * Put.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers a put operation
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
   PbeerCommon at 'PbeerCommon.ozf'
export
   DefArgs
   Run
define
   DefArgs = nil

   proc {Run Args}
      Pbeer
      Key
      PrintKey
   in
      if Args.help then
         {PbeerBaseArgs.helpMessage [key value cap ring store] nil put}
         {Application.exit 0}
      end
      Pbeer = {PbeerCommon.getPbeer Args.store Args.ring}
      Key#PrintKey = {PbeerCommon.capOrKey Args.cap Args.key}
      {Pbeer put(Key Args.value)}
      {Delay 100}
      {System.showInfo "Operation put("#PrintKey#" "#Args.value#") sent."}
      {Application.exit 0}
   end
end
