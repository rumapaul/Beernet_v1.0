/*-------------------------------------------------------------------------
 *
 * KeyRanges.oz
 *
 *    Procedures for a clockwise circular identifier address space.
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
   Random   at 'Random.ozf'
export
   BelongsTo
   CheckOrder
   GetRandomKey
   GetUniqueKey
   InsertPeer
   InsertPeerWithOmega
   ChordIdFingers
   KaryIdFingers
   Log
define

   %% Use process id to seed the random numbers, but do it only once.
   %% So, if the pbeer is not alone, it is worng to reset the seed.
   OnlyPbeer = {NewCell true}

   %% Check if a Key is in between From and To considering circular ranges
   fun {BelongsTo Key From To}
      if From < To then
         From < Key andthen Key =< To
      else
         From < Key orelse Key =< To
      end
   end

   %% Boolean. Check the serie Id->P->Q clockwise
   fun {CheckOrder Id P Q}
      {BelongsTo P Id Q}
   end

   %% Random Key generator
   fun {GetRandomKey NetworkSize}
      State
      NewState
   in
      State = OnlyPbeer := NewState
      if State then
         {Random.setSeed}
      end
      NewState = false
      {Random.urandInt 0 NetworkSize}
   end

   %% Make sure that the randomly generated key is not already taken
   %% by another node. All the generated Ids should be included in AddBook.
   %% This function is only useful in simulation mode with a global view.
   fun {GetUniqueKey NetworkSize AddBook}
      Key = {GetRandomKey NetworkSize}
   in
      if {Dictionary.member AddBook Key} then
         {GetUniqueKey NetworkSize AddBook}
      else
         Key
      end
   end

   %% Insert New peer circular-clockwise in a sorted list of Peers.
   %% Pivot is the starting point of clockwise order.
   %% N is the size of the address space.
   %% Do not insert the peer if it is already in the list
   %% Returns the new list with the New peer inserted
   fun {InsertPeer New Pivot N Peers}
      fun {Relative Id}
         ((Id - Pivot) + N) mod N
      end
   in
      case Peers
      of Peer|Rest then
         if {Relative New.id} < {Relative Peer.id} then
            New|Peers
         elseif New.id == Peer.id then
            Peers
         else
            Peer|{InsertPeer New Pivot N Rest}
         end
      [] nil then
         [New]
      end
   end

   %% Do not insert more peers than Omega
   %% This algorithm is useful in case we decide to use Palta topology
   fun {InsertPeerWithOmega New Pivot N Peers Omega}
      if {List.length Peers} > Omega then
         Peers
      else
         {InsertPeer New Pivot N Peers}
      end
   end

   %% According to Id and N, it returns a list of ids for fingers
   fun {ChordIdFingers Id N}
      {KaryIdFingers Id 2 N}
   end

   %% According to Id, K and N, it returns a list of K-ary ids for fingers
   fun {KaryIdFingers Id K N}
      fun {KLoop D I Acc}
         if I == 0 then
            Acc
         else
            {KLoop D I-1 ((Id + D * I) mod N)|Acc}
         end
      end
      fun {Loop I Acc}
         D = N div I
      in
         if D > 0 then
            {Loop I*K {KLoop D K-1 Acc}}
         else
            Acc
         end
      end    
   in
      {Loop K nil}
   end

   %% Integer version of logarithmic function (approximated)
   fun {Log Base Value}
      fun {Loop I Acc}
         NewAcc = Acc*Base
      in
         if NewAcc >= Value then
            I
         else
            {Loop I+1 NewAcc}
         end
      end
   in
      {Loop 1 1}
   end
end
