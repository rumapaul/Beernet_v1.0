/*-------------------------------------------------------------------------
 *
 * HashedList.oz
 *
 *    This files contains add/remove operations for lists containing values
 *    sorted by hash function. It does not contain duplicated values. Hashed
 *    values can be duplicated.
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
export
   Add
   Remove
   GetValues
define

   fun {Add L X HX}
      case L
      of Val|MoreValues then
         if HX < Val.hash then
            v(value:X hash:HX)|L
         elseif X == Val.value then
            L
         else
            Val|{Add MoreValues X HX}
         end
      [] nil then
         [v(value:X hash:HX)]
      end
   end

   fun {Remove L X HX}
      case L
      of Val|MoreValues then
         if X == Val.value then
            MoreValues
         elseif HX < Val.hash then
            L
         else
            Val|{Remove MoreValues X HX}
         end
      [] nil then
         nil
      end
   end

   fun {GetValues L}
      case L
      of H|T then
         H.value|{GetValues T}
      [] nil then
         nil
      end
   end
end
