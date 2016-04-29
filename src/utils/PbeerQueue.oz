/*-------------------------------------------------------------------------
 *
 * PbeerQueue.oz
 *
 * This files contains general functions asociated with queue.
 * The queue may contain any single or compound element
 *
 * LICENSE
 *
 * Beernet is released under the Beerware License (see file LICENSE)
 *
 * IDENTIFICATION
 *
 * Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 * Last change: $Revision: 5 $ $Author: ruma $
 *
 * $Date: 2014-11-05 13:59:41 +0200 (Wed, 05 Nov 2014) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   Enqueue
   Dequeue
   IsEmpty
   IsInQueue
   New
   
define

   %% Add an element at the end of list.
   %% Return the new list as result
   fun {Enqueue Element L}
      case L
      of H|T then
            H|{Enqueue Element T}
      [] nil then
         Element|nil
      end
   end

   fun {IsInQueue P L}
      case L
      of H|T then
         case H of element(Q _) then
              if Q.id == P.id then
                 true
              else
                 {IsInQueue P T}
              end
         else
              {IsInQueue P T}
         end
     [] nil then
         false
     end
   end

   %% Remove the last element of a list
   fun {Dequeue L Current}
      case L
      of H|T then
         Current := H
         T
      [] nil then
         nil
      end
   end

   fun {IsEmpty L}
     case L
     of H|_ then
        case H of element(_ _) then
            false
        else
            true
        end
     [] nil then
        true
     end
   end

   %% For the sake of completeness of the ADT
   fun {New}
      nil
   end
end
