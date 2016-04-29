/*-------------------------------------------------------------------------
 *
 * PbeerIdList.oz
 *
 *    This files contains general functions asociated with list of PBeer Ids. 
 *    Lists are sorted. It can also be used for a sorted list of integers.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 10 $ $Author: ruma $
 *
 *    $Date: 2015-01-29 19:31:46 +0200 (Thurs, 29 Jan 2015) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   AddId
   RemoveId
   IsIdIn
   GetIndex
   Minus
   Union
   New

define

   fun {AddId PeerId L}
      case L
      of H|T then
         if H < PeerId then
            H|{AddId PeerId T}
         elseif H == PeerId then
            L
         else
            PeerId|L
         end
      [] nil then
         PeerId|nil
      end
   end

   %% Return true if PeerId is found in list L
   %% Precondition: L is sorted
   fun {IsIdIn PeerId L}
      case L
      of H|T then
         if H == PeerId then
            true
         elseif H < PeerId then
            {IsIdIn PeerId T}
         else
            false
         end
      [] nil then
         false
      end
   end

   %% Return index if PeerId is found in list L, otherwise return 0
   %% Precondition: L is sorted
   fun {GetIndex PeerId L Index}
      case L
      of H|T then
         if H == PeerId then
            Index
         elseif H < PeerId then
            {GetIndex PeerId T Index+1}
         else
            0
         end
      [] nil then
         0
      end
   end

    %% Return id who is responsible for given key
   %% Precondition: L is sorted
   fun {GetResponsible PeerId L}
      case L
      of H|T then
         if PeerId =< H then
            H
         else
            {GetResponsible PeerId T}
         end
      [] nil then
         nil
      end
   end

   %% Remove a Peer from a List
   %% Precondition: L is sorted
   fun {RemoveId PeerId L}
      case L
      of H|T then
         if H == PeerId then
            T
         elseif H < PeerId then
            H|{RemoveId PeerId T}
         else
            L
         end
      [] nil then
         nil
      end
   end

   %% Return a list with elements of L1 that are not present in L2
   %% Precondition: L1 and L2 are sorted
   fun {Minus L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1 == H2 then
            {Minus T1 T2}
         elseif H1 < H2 then
            H1|{Minus T1 L2}
         else
            {Minus L1 T2}
         end
      [] nil#_ then
         nil
      [] _#nil then
         L1
      end
   end

   %% Return a list with all elements from L1 and L2. 
   %% Do not duplicate elements
   %% Precondition: L1 and L2 are sorted
   fun {Union L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1 == H2 then
            H1|{Union T1 T2}
         elseif H1 < H2 then
            H1|{Union T1 L2}
         else
            H2|{Union L1 T2}
         end
      [] nil#_ then
         L2
      [] _#nil then
         L1
      end
   end

   %% For the sake of completeness of the ADT
   fun {New}
      nil
   end
end
