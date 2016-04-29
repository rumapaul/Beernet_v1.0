/*-------------------------------------------------------------------------
 *
 * PbeerList.oz
 *
 *    This files contains general functions asociated with list, but actually,
 *    they work some times as if they were sets. Lists are sorted
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
 *    Last change: $Revision: 410 $ $Author: ruma $
 *
 *    $Date: 2014-08-20 17:09:04 +0200 (Wed, 20 Aug 2014) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   Add
   Edit
   Different
   Intersection
   IsIn
   Keep
   KeepAndDrop
   Minus
   New
   Remove
   RemoveId
   RemoveLast
   RetrievePbeer  
   Union
define

   %% Add a Peer in a sorted list.
   %% Return the new list as result
   fun {Add Peer L}
      case L
      of H|T then
         if H.id < Peer.id then
            H|{Add Peer T}
         elseif H.id == Peer.id then
            %% Compare the whole peer, because it could be they have same id
            %% but different ports
            L
         else
            Peer|L
         end
      [] nil then
         Peer|nil
      end
   end

   %% Edit a Peer in a sorted list.
   %% Return the new list as result
   fun {Edit Peer L}
      case L
      of H|T then
         if H.id < Peer.id then
            H|{Edit Peer T}
         elseif H.id == Peer.id then
            Peer|T
         else
            Peer|L
         end
      [] nil then
         Peer|nil
      end
   end

   %% Compare if two lists are diferent
   fun {Different L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1.id \= H2.id then
            true
         else
            {Different T1 T2}
         end
      [] nil#nil then
         false
      [] nil#_ then
         true
      [] _#nil then
         true
      end
   end

   %% Return true if Peer is found in list L
   %% Precondition: L is sorted
   fun {IsIn Peer L}                       %Fixed a bug by R
      case L
      of H|T then
         if H.id == Peer.id then
            true
         elseif H.id < Peer.id then
            {IsIn Peer T}
         else
            false
         end
      [] nil then
         false
      end
   end

   fun {RetrievePbeer PeerId L}
      case L
      of H|T then
         if H.id == PeerId then
	    H
         elseif H.id < PeerId then
            {RetrievePbeer PeerId T}
         else
            nil
         end
      [] nil then
         nil
      end
   end

   %% Return a list with the intersection between the two lists
   fun {Intersection L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1.id == H2.id then
            H1|{Intersection T1 T2}
         elseif H1.id < H2.id then
            {Intersection T1 L2}
         else
            {Intersection L1 T2}
         end
      [] nil#_ then nil
      [] _#nil then nil
      end
   end

   %% Like Take
   fun {Keep N L}
      case L
      of H|T then
         if N > 0 then
            H|{Keep N-1 T}
         else
            nil
         end
      [] nil then
         nil
      end
   end         
  
   %% Return the list of kept elements AND the list of dropped elements
   fun {KeepAndDrop N L Drop}
      case L
      of H|T then
         if N > 0 then
            H|{KeepAndDrop N-1 T Drop}
         else
            Drop = T
            nil
         end
      [] nil then
         Drop = nil
         nil
      end
   end         
   
   %% Remove a Peer from a List
   fun {Remove Peer L}
      case L
      of H|T then
         if H.id == Peer.id then
            T
         else
            H|{Remove Peer T}
         end
      [] nil then
         nil
      end
   end

   %% Remove a Peer from a List
   fun {RemoveId PeerId L}
      case L
      of H|T then
         if H.id == PeerId then
            T
         else
            H|{Remove PeerId T}
         end
      [] nil then
         nil
      end
   end

   %% Remove the last element of a list
   fun {RemoveLast L}
      case L
      of _|nil then
         nil
      [] H|T then
         H|{RemoveLast T}
      [] nil then
         nil
      end
   end

   %% Return a list with elements of L1 that are not present in L2
   %% Precondition: L1 and L2 are sorted
   fun {Minus L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1.id == H2.id then
            {Minus T1 T2}
         elseif H1.id < H2.id then
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

   %% For the sake of completeness of the ADT
   fun {New}
      nil
   end

   %% Return a list with all elements from L1 and L2. 
   %% Do not duplicate elements
   %% Precondition: L1 and L2 are sorted
   fun {Union L1 L2}
      case L1#L2
      of (H1|T1)#(H2|T2) then
         if H1.id == H2.id then
            H1|{Union T1 T2}
         elseif H1.id < H2.id then
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


end
