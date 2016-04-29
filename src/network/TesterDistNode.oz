functor

import
   Application
   Connection
   Pickle
   System
   Network     at 'Network.ozf'
   TestPlayers at 'TestPlayers.ozf'

define

   ComLayer
   Coordinator
   MySite
   Finish
   OtherPlayer
   Args = try
             {Application.getArgs
              record(coordinator(single type:atom default:none)
                     id(single type:atom default:none)
                     start(single default:false)
                     total(single char:n type:int default:10)
                    )}
          catch _ then
             {System.showInfo 'Unrecognised arguments'}
             {Application.exit 1}
          end
in

   MySite = {TestPlayers.makeNetworkPingPongPlayer}
   {MySite setId(Args.id)}
   {MySite setFlag(Finish)}
   Coordinator = {Connection.take {Pickle.load Args.coordinator}}
   ComLayer = {Network.new}
   if Args.start then
      {System.show Args.id#' is going to start testing Network PinPong'}
      {ComLayer sendTo(Coordinator starter({MySite getRef($)} OtherPlayer))}
      {Wait OtherPlayer}
      {MySite initPing(Args.total OtherPlayer)}
      {Wait Finish}
      {System.show Args.id#'wins Network PingPong'}
      {ComLayer sendTo(Coordinator finish)}
      {Application.exit 0}
   else
      {ComLayer sendTo(Coordinator simple({MySite getRef($)} OtherPlayer))}
      {Wait OtherPlayer}
      {MySite setOtherPlayer(OtherPlayer)}
      {Wait Finish}
      {System.show Args.id#' wins Network PingPong'}
      {ComLayer sendTo(Coordinator finish)}
      {Application.exit 0}
   end

end
