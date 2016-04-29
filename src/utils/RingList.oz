/*-------------------------------------------------------------------------
 *
 * RingList.oz
 *
 *    Collection of procedures to work with lists of peers on a circular
 *    address space sorted with a pivot.
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
 *    Last change: $Revision: 405 $ $Author: ruma $
 *
 *    $Date: 2014-11-06 23:57:55 +0200 (Thurs, 6 Nov 2014) $
 *
 *-------------------------------------------------------------------------
 */

functor
import
   PbeerList   at 'PbeerList.ozf'
export
   Add
   Distance
   Different
   ForAll
   ForOne
   GetAfter
   GetBefore
   GetFirst
   GetLast
   Keep
   KeepAndDrop
   IsEmpty
   IsIn
   Merge
   Minus
   New
   NewPivot
   Remove
   RemoveLast
   Tail
define
   Keep        = PbeerList.keep
   KeepAndDrop = PbeerList.keepAndDrop
   RemoveLast  = PbeerList.removeLast

   fun {Distance Id Pivot MaxKey}
      (MaxKey + Id - Pivot) mod MaxKey
   end

   %% Add a Peer in a sorted list with a distance relative to the pivot.
   %% Do not add a peer if it is already in the list.
   %% Return the new list as result
   fun {Add NewPeer L Pivot MaxKey}
      fun {Loop NewDist L}
         case L
         of (Dist#Peer)|Tail then
            if Dist < NewDist then
               (Dist#Peer)|{Loop NewDist Tail}
            elseif Dist == NewDist then
               L
            else
               (NewDist#NewPeer)|L
            end
         [] nil then
            (NewDist#NewPeer)|nil
         end
      end
      TheDistance = {Distance NewPeer.id Pivot MaxKey}
   in
      if TheDistance == 0 then
         {Loop MaxKey L}
      else
         {Loop TheDistance L}
      end
   end

   %% Compare if two lists are diferent
   fun {Different L1 L2}
      case L1#L2
      of (_/*Dist1*/#H1|T1)#(_/*Dist2*/#H2|T2) then
         if H1.id \= H2.id then
            true
         else
            {Different T1 T2}
         end
      [] nil#nil then
         false
      [] nil#_/*L2*/ then
         true
      [] _/*L1*/#nil then
         true
      end
   end

   proc {ForAll L P}
      case L
      of (_/*Dist*/#Peer)|T then
         {P Peer}
         {ForAll T P}
      [] nil then
         skip
      end
   end

   proc {ForOne L P DiscardedPeer}
      case L
      of (_/*Dist*/#Peer)|T then
         if Peer.id \= DiscardedPeer.id then
            {P Peer}
         else
            {ForOne T P DiscardedPeer}
         end
      [] nil then
         skip
      end
   end

   fun {GetAfter Id L Pivot MaxKey}
      fun {Loop RelId L}
         case L
         of (Dist#Peer)|T then
            if Dist >= RelId then
               Peer %% It's mee
            else
               {Loop RelId T}
            end
         [] nil then
            nil
         end
      end
   in
      {Loop {Distance Id Pivot MaxKey} L}
   end
        
   fun {GetBefore Id L Pivot MaxKey}
      fun {Loop RelId L Candidate}
         case L
         of (Dist#Peer)|T then
            if Dist > RelId then
               Candidate %% It's the previous one
            else
               {Loop RelId T Peer}
            end
         [] nil then
            Candidate
         end
      end
   in
      {Loop {Distance Id Pivot MaxKey} L nil}
   end

   fun {GetFirst L Default}
      case L
      of (_/*Dist*/#Peer)|_/*Tail*/ then
         Peer
      else
         Default
      end
   end

   fun {GetLast L Default}
      case L
      of (_/*Dist*/#Peer)|nil then
         Peer
      [] _/*Dist#Peer*/|T then
         {GetLast T Default}
      else
         Default
      end
   end

   fun {IsEmpty L}
      L == nil
   end

   %% Return true if Peer is found in list L
   fun {IsIn Peer L}
      case L
      of (_/*Dist*/#H)|T then
         if H.id == Peer.id then
            true
         else
            {IsIn Peer T}
         end
      [] nil then
         false
      end
   end

   %% Like Take
   %% Keep function is exatly as in CiList
   
   %% Return the unification of two list respecting the order
   fun {Merge L1 L2}
      case L1#L2
      of ((Dist1#Peer1)|T1)#((Dist2#Peer2)|T2) then
         if Peer1.id == Peer2.id then
            (Dist2#Peer2)|{Merge T1 T2}
         elseif Dist1 > Dist2 then
            (Dist2#Peer2)|{Merge L1 T2}
         else
            (Dist1#Peer1)|{Merge T1 L2}
         end
      [] nil#_ then
         L2
      [] _#nil then
         L1
      end
   end

   %% Return a list with elements of L1 that are not present in L2
   fun {Minus L1 L2}
      case L1
      of (Dist#Peer)|T then
         if {IsIn Peer L2} then
            {Minus T L2}
         else
            (Dist#Peer)|{Minus T L2}
         end
      [] nil then
         nil
      end
   end

   %% For the sake of completeness of the ADT
   fun {New}
      nil
   end

   %% Changes the distance of the list members according to the new pivot.
   %% It sorted the peers if necessary.
   fun {NewPivot L Pivot MaxKey}
      proc {Loop L LastDist FilteredEnd Rest}
         case L
         of (_#Peer)|Tail then
            NewDist = {Distance Peer.id Pivot MaxKey}
         in
            if NewDist > LastDist  then
               NewEnd
            in
               FilteredEnd = (NewDist#Peer)|NewEnd 
               {Loop Tail NewDist NewEnd Rest}
            elseif NewDist == 0 then
               NewEnd
            in
               FilteredEnd = (NewDist#Peer)|NewEnd
               {Loop Tail MaxKey NewEnd Rest}
            else
               FilteredEnd = nil
               Rest = L
            end
         [] nil then
            FilteredEnd = nil
            Rest = nil
         end
      end
      Filtered
      NewHead
      NewFilteredHead
   in
      {Loop L 0 Filtered NewHead}
      {Loop NewHead 0 NewFilteredHead nil}
      {Append NewFilteredHead Filtered}
   end

   %% Remove a Peer from a List
   fun {Remove Peer L}
      case L
      of (Dist#HeadPeer)|Tail then
         if HeadPeer.id == Peer.id then
            Tail
         else
            (Dist#HeadPeer)|{Remove Peer Tail}
         end
      [] nil then
         nil
      end
   end

   fun {Tail L}
      L.2
   end
end   
