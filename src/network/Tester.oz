%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   System
   TestPlayers at 'TestPlayers.ozf'

define
   
   local
      SiteA
      SiteB
      Finish
   in
      SiteA = {TestPlayers.makePp2pPingPongPlayer}
      SiteB = {TestPlayers.makePp2pPingPongPlayer}
      {SiteA setId(foo)}
      {SiteB setId(bar)}
      {SiteA setFlag(Finish)}
      {SiteB setFlag(Finish)}
      {System.show 'going to start testing Pp2pPinPong'}
      {SiteA initPing(10 {SiteB getRef($)})}
      {Wait Finish}
      {System.show 'finishing Pp2pPingPong'}
   end

   local
      SiteA
      SiteB
      Finish
   in
      SiteA = {TestPlayers.makeNetworkPingPongPlayer}
      SiteB = {TestPlayers.makeNetworkPingPongPlayer}
      {SiteA setId(netfoo)}
      {SiteB setId(netbar)}
      {SiteA setFlag(Finish)}
      {SiteB setFlag(Finish)}
      {System.show 'starting NetworkPinPong test'}
      {SiteB setOtherPlayer({SiteA getRef($)})}
      {SiteA initPing(10 {SiteB getRef($)})}
      {Wait Finish}
      {System.show 'finishing NetworkPingPong'}
   end
   {Application.exit 1}
end
