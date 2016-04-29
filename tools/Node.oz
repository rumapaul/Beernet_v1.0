/*-------------------------------------------------------------------------
 *
 * Note.oz
 *
 *    This program launches a beernet peer, and it allows to kill it. It is
 *    rarely used manually. It is mostly called from programs beernet and
 *    pbeer.
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
   Property
   BaseArgs    at '../lib/tools/BaseArgs.ozf'
   Clansman    at '../lib/tools/Clansman.ozf'
define

   Args
   Help  = ["  -m, --master\tStores the ring reference (default: false)"
            "  -b, --busy\tKeeps the peer busy printing its neighbours"]
   DefArgs = record(
                     busy(single       char:&b  type:bool   default:false)
                     master(single     char:&m  type:bool   default:false)
                   )
in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   Args = {BaseArgs.getArgs DefArgs}

   %% Help message
   if Args.help then
      {BaseArgs.helpMessage Help}
      {Application.exit 0}
   end

   {Clansman.run Args}

end
