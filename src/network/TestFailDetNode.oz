functor
import
   Application
   Connection
   Pickle
   System
   Board             at '../corecomp/Board.ozf'
   Component         at '../corecomp/Component.ozf'
   Network           at 'Network.ozf'
   FailureDetector   at 'FailureDetector.ozf'
define

   fun {MakeNode Id}

      Self
      SelfRef
      ComLayer

      proc {ConnectTo Event}
         connectTo(Pbeer) = Event
      in
         {ComLayer monitor(Pbeer)}
         {ComLayer sendTo(Pbeer ping(@SelfRef))}
         {System.show 'Trying to connect to '#Pbeer}
      end

      proc {Crash Event}
         crash(Pbeer) = Event
      in
         {System.showInfo "Pbeer "#Pbeer.id#" has crashed!"}
      end

      proc {Ping Event}
         ping(Pbeer) = Event
      in
         {System.showInfo "Pbeer "#Pbeer.id#" is trying to contact me"}
         {ComLayer monitor(Pbeer)}
         {ComLayer sendTo(Pbeer pong(@SelfRef))}
      end

      proc {Pong Event}
         pong(Pbeer) = Event
      in
         {System.showInfo "I got contact with "#Pbeer.id}
      end

      proc {SetRef Event}
         setRef(Ref) = Event
      in
         SelfRef := Ref
      end

      proc {ToTicket Event}
         toTicket(FileName) = Event
      in
         {Pickle.save {Connection.offerUnlimited @SelfRef} FileName}
      end

      Events = events(
                  connectTo:  ConnectTo
                  crash:      Crash
                  ping:       Ping
                  pong:       Pong
                  setRef:     setRef
                  toTicket:   ToTicket
                  )
   in
      Self     = {Component.newTrigger Events}
      ComLayer = {Network.new}
      {ComLayer setId(Id)}
      {ComLayer setListener(Self)}
      SelfRef = {Cell.new {ComLayer getRef($)}}
      Self
   end

   Args = try
             {Application.getArgs
              record(id(single type:atom default:none)
                    )}
          catch _ then
             {System.showInfo 'Unrecognised arguments'}
             {Application.exit 1}
          end
   ThisNode
in
   ThisNode = {MakeNode Args.id}
   {ThisNode toTicket(Args.id)}
end
