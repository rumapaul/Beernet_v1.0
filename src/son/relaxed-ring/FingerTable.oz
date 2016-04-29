/*-------------------------------------------------------------------------
 *
 * FingerTable.oz
 *
 *    K-ary finger table to route message in O(log_k(N)) hops.
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
 *    $Date: 2016-04-26 15:30:57 +0200 (Tues, 26 April 2016) $
 *
 * NOTES
 *      
 *     This Finger Table is based on DKS generalization of Chord fingers to
 *     guarantee O(log_k(N)) hops (at the level of the overlay, not counting
 *     tcp/ip connections). The idea is that the address space of size N is
 *     divided in k, and then, the smallest fraction is divided again into k,
 *     until the granularity is small enough.
 *
 *     The Finger Table does not receives messages from the comunication
 *     layer. It only sends messages through it. Messages are receive by the
 *     Finger Table from within the node using the event route(...).
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   BootTime at 'x-oz://boot/Time'
   Component   at '../../corecomp/Component.ozf'
   KeyRanges   at '../../utils/KeyRanges.ozf'
   RingList    at '../../utils/RingList.ozf'
   Utils       at '../../utils/Misc.ozf'
   Constants   at '../../commons/Constants.ozf'
   
export
   New
define

   %% Default values
   K_DEF    = Constants.fingerTableK         % Factor k for k-ary fingers

   fun {New Args}
      Self
      Id          % Id of the owner node. Pivot for relative ids
      Fingers     % RingList => sorted using Id first reference
      IdealIds    % Ideals ids to chose the fingers
      K           % Factor k to divide the address space to choose fingers
      MaxKey      % Maximum value for a key
      Node        % The Node that uses this finger table
      NodeRef     % Node's reference
      Refresh     % Flag to know if refreshing is finished
      Refreshing  % List of acknowledged ids
      RefreshTS   % Timestamp of last refreshing
      FTHealth    % Missing Fingers in the partitions
      HasFTConverged % true if old FTHealth is same as current within a time period
      FTHealthTS  % Timestamp for FT Health check

      ComLayer    % Communication Layer, to send messages.

      % --- Utils ---
      fun {CheckNewFinger Ids Fgs New}
         case Ids
         of H|T then
            if {RingList.isEmpty Fgs} then
               {RingList.add New Fgs @Id @MaxKey}
            else
               P  = {RingList.getFirst Fgs noFinger}
               Ps = {RingList.tail Fgs}
            in
               if {KeyRanges.checkOrder @Id H P.id} then
                  if {KeyRanges.checkOrder @Id H New.id} then
                     if {KeyRanges.checkOrder @Id New.id P.id} then
                        {RingList.add New {CheckNewFinger T Ps P} @Id @MaxKey}
                     else
                        {RingList.add P {CheckNewFinger T Ps New} @Id @MaxKey}
                     end
                  else
                     Fgs
                  end
               else
                  {CheckNewFinger Ids Ps New}
               end
            end
         [] nil then
            {RingList.new}
         end
      end

      fun {ClosestPrecedingFinger Key}
         {RingList.getBefore Key @Fingers @Id @MaxKey}
      end

      %% --- Events --- 
      proc {AddFinger addFinger(Pbeer)}
         Fingers := {CheckNewFinger @IdealIds @Fingers Pbeer}
      end

      proc {FindFingers Event}
         findFingers(_/*Contact*/) = Event
      in
         skip
      end

      proc {GetFingers getFingers(TheFingers)}
         TheFingers = @Fingers
      end

      proc {GetRefreshTS getRefreshTS(LastRefresh)}
	LastRefresh = @RefreshTS
      end

      proc {GetFTHealth getFingersStatus(FTStatus ConvergenceFlag)}
        if @HasFTConverged andthen @FTHealthTS \= nil andthen {BootTime.getReferenceTime}-@FTHealthTS < Constants.fTStatusValidityPeriod then
		FTStatus = @FTHealth
		ConvergenceFlag = @HasFTConverged
        else
		OldFTHealth = @FTHealth
		TotalFingers = {List.length @IdealIds}
		HighestCoeff = {Int.toFloat TotalFingers}/{Int.toFloat (@K-1)}
		F_K = {Int.toFloat @K}
		MissingFingers = {NewCell 0.0}
		Weight = {NewCell 1.0/{Number.pow F_K HighestCoeff}}
		PrevId = {NewCell nil}
		Diff = {NewCell 0}
		in
                if @RefreshTS==nil orelse ({BootTime.getReferenceTime}-@RefreshTS)>Constants.fTValidityPeriod then
			Flag
			in
			{RefreshFingers refreshFingers(Flag)}
			{Wait Flag}
		end
		for PId in @IdealIds do
			if @PrevId \= nil then
				FoundFinger = {RingList.getAfter @PrevId @Fingers @Id @MaxKey}
				in
                		if @Diff==0 then
					Diff := PId - @PrevId
				else
					if PId-@PrevId > @Diff then
						Diff := PId-@PrevId
						Weight := @Weight * F_K
					end
				end	 
				if FoundFinger==nil then
					MissingFingers := @MissingFingers + @Weight
				else
					if {Not {KeyRanges.checkOrder @PrevId FoundFinger.id PId}} then
						MissingFingers := @MissingFingers + @Weight
					end
				end
			end
			PrevId := PId		
		end
		local
			FoundFinger = {RingList.getAfter @PrevId @Fingers @Id @MaxKey}
			in
			if FoundFinger==nil then
				MissingFingers := @MissingFingers + @Weight
			else
				if {Not {KeyRanges.checkOrder @PrevId FoundFinger.id @Id}} then
					MissingFingers := @MissingFingers + @Weight
				end
			end
		end
        	FTHealth := @MissingFingers * 100.0
                if OldFTHealth\=nil andthen {Number.abs (@FTHealth-OldFTHealth)}<Constants.fTValidityFactor then
			HasFTConverged := true
		else
			HasFTConverged := false
		end
		FTHealthTS := {BootTime.getReferenceTime}
		FTStatus = @FTHealth
		ConvergenceFlag = @HasFTConverged
	end
      end

      proc {Monitor monitor(Pbeer)}
         Fingers := {CheckNewFinger @IdealIds @Fingers Pbeer}
      end

      proc {NeedFinger needFinger(src:Src key:K)}
         {@Node dsend(to:Src newFinger(key:K src:@NodeRef))}
      end

      proc {NewFinger newFinger(key:K src:Pbeer)}
         Fingers     := {CheckNewFinger @IdealIds @Fingers Pbeer}
         Refreshing  := {Utils.deleteFromList K @Refreshing} 
         if @Refreshing == nil then %% Got all refreshing fingers answers
            @Refresh = unit
	    RefreshTS := {BootTime.getReferenceTime}
         end
      end

      proc {RefreshFingers refreshFingers(Flag)}
         Refreshing  := @IdealIds
         @Refresh    = Flag
         for K in @IdealIds do
            {@Node route(msg:needFinger(src:@NodeRef key:K) src:@NodeRef to:K)}
         end
      end

      proc {RemoveFinger removeFinger(Finger)}
         Fingers := {RingList.remove Finger @Fingers}
      end

      proc {Route Event}
         Msg = Event.msg
	 Src = Event.src
	 Target = Event.to
         ClosestTarget
      in
         if {Not {Record.label Msg} == join} then
            {Monitor monitor(Src)}
         end
         ClosestTarget = {ClosestPrecedingFinger Target}
         if ClosestTarget \= nil then
            {@ComLayer sendTo(ClosestTarget Event)}
         else
            {@ComLayer sendTo({@Node getSucc($)} Event)}
         end
      end

      proc {SetComLayer setComLayer(NewComLayer)}
         ComLayer := NewComLayer
      end

      proc {SetId setId(NewId)}
         Id := NewId
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      proc {SetK setK(NewK)}
         K := NewK
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      proc {SetMaxKey setK(NewMaxKey)}
         MaxKey := NewMaxKey
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      Events = events(
                  addFinger:     AddFinger
                  findFingers:   FindFingers
                  getFingers:    GetFingers
		  getFingersStatus:	GetFTHealth
	          getRefreshTS:	 GetRefreshTS
                  monitor:       Monitor
                  needFinger:    NeedFinger
                  newFinger:     NewFinger
                  refreshFingers:RefreshFingers
                  removeFinger:  RemoveFinger
                  route:         Route
                  closestPreceedingRoute: Route
                  setComLayer:   SetComLayer
                  setId:         SetId
                  setMaxKey:     SetMaxKey
                  setK:          SetK
                  )
   in %% --- New starts ---
      Self        = {Component.newTrigger Events}
      K           = {NewCell K_DEF}
      Node        = {NewCell Args.node}
      MaxKey      = {NewCell {@Node getMaxKey($)}}
      Id          = {NewCell {@Node getId($)}}
      NodeRef     = {NewCell {@Node getRef($)}}
      IdealIds    = {NewCell {KeyRanges.karyIdFingers @Id @K @MaxKey}}
      Fingers     = {NewCell {RingList.new}}
      Refreshing  = {NewCell nil}
      Refresh     = {NewCell _}
      RefreshTS   = {NewCell nil}
      FTHealth    = {NewCell nil}
      HasFTConverged = {NewCell false}
      FTHealthTS  = {NewCell nil}
      ComLayer    = {NewCell Component.dummy}
      Self
   end

end
