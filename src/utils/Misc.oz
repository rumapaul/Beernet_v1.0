/*-------------------------------------------------------------------------
 *
 * Misc.oz
 *
 *    Miscellaneous procedures that can be useful to other modules
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *            Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 420 $ $Author: ruma $
 *
 *    $Date: 2013-07-30 14:51:17 +0200 (Tues, 30 July 2013) $
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Pickle
   System
export
   AddDefaults
   Blabla
   DelegatesTo
   DeleteFromList
   Hash
define

   %% Add default fields to a record.
   %% Rec is the input record
   %% Defaults follows the pattern: def(field1:value1 ... fieldN:valueN)
   fun {AddDefaults Rec Defaults}
      fun {AddingLoop Fields Acc}
         case Fields
         of Field|MoreFields then
            NewAcc
         in
            if {HasFeature Rec Field} then
               NewAcc = {Record.adjoinAt Acc Field Rec.Field}
            else
               NewAcc = {Record.adjoinAt Acc Field Defaults.Field}
            end
            {AddingLoop MoreFields NewAcc} 
         [] nil then
            Acc
         end
      end
   in
      {AddingLoop {Arity Defaults} Rec}
   end

   %% algorithm from http://www.cse.yorku.ca/~oz/hash.html
   %% Returns a hash value for N between 0 and Max
   fun {Hash N Max}
      B = {VirtualByteString.toCompactByteString {Pickle.pack N}}
      L = {ByteString.length B}
      fun{Loop I Old}
         if I < L then
            {Loop I + 1 ((Old*33) + {ByteString.get B I}) mod Max}
         else
            Old
         end
      end
   in
      {Loop 0 5381}
   end

   %%--- Delete an element from a list, returning the new list ---
   fun {DeleteFromList E L}
      case L
      of !E|T then T
      [] H|T then H|{DeleteFromList E T}
      [] nil then nil
      end
   end

   %%--- Make Delegators ---
   fun {DelegatesTo Comp}
      proc {$ Event}
         {@Comp Event}
      end
   end

   %%--- For debug information ---
   proc {Blabla Text}
      try
         {System.showInfo Text}
      catch _ then
         {System.show Text}
      end
   end

/*
   proc {Blabla _}
      skip
   end
*/

end

