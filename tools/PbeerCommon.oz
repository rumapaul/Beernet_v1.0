/*-------------------------------------------------------------------------
 *
 * PbeerCommon.oz
 *
 *    Common functionality shared by several 'pbeer' subcommands.
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
   Connection
   Pickle
   PbeerBaseArgs  at 'PbeerBaseArgs.ozf'
export
   CapOrKey
   GetCapOrKey
   GetPbeer
define

   fun {GetPbeer StoreTket RingKey}
      Mordor
   in
      Mordor = {Connection.take {Pickle.load StoreTket}}
      {Send Mordor getPbeer(RingKey $)}
   end

   fun {CapOrKey CapFile Key}
      if CapFile \= {PbeerBaseArgs.getDefault cap} then
         Cap
      in
         try
            Cap = {Pickle.load CapFile}
         catch _ then %% The CapFile does not exist. Create new cap
            Cap = {Name.new}
            {Pickle.save Cap CapFile}
         end
         Cap#("<"#CapFile#">")
      else
         Key#Key
      end
   end

   fun {GetCapOrKey CapFile Key}
      if CapFile \= {PbeerBaseArgs.getDefault cap} then
         {Pickle.load CapFile}#("<"#CapFile#">")
      else
         Key#Key
      end
   end

end

