/*-------------------------------------------------------------------------
 *
 * socketServer 
 *
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Application
   Open
   OS
   Property
   System
   AdhocParser at '../lib/tools/AdhocParser.ozf'
   PbeerCommon at '../lib/tools/PbeerCommon.ozf'
define
   DEFAULTPORT = 6666
   PUT = rec(success(0) error(1))
   GET = rec(any_value 'NOT_FOUND')
   RING_NAME   = eldorado
   STORE_TKET  = 'mordor.tket'

   Parse    = AdhocParser.parse %% To parse strings that arrived on the socket
   Say      = System.showInfo %% For feedback to the standard output
   Pbeer    %% Part of the network. Used to run the requests
   Blabla   %% For verbose feedback
   Bla      %% For verbose feedback without newline
   Args     %% Application arguments
   Server   %% THIS socket connection for listening other programs

   fun {GetVS Set Field}
      {Value.toVirtualString Set.Field 100 100}
   end

   %% Behaviour of the socket server is defined here
   class Accepted from Open.socket 

      meth readLoop
         TheMsg
         Request
      in
         try
            {self flush}
            {self read(list:TheMsg)}
            {Bla "Got "#TheMsg}
            Request = {Parse TheMsg}
            case Request
            of error(E) then
               {Bla E}
               {self toSocket("Wrong operation! try again!")}
            else
               Result
            in
               if {Label Request} == get then
                  {Pbeer {Record.adjoinAt Request v Result}}
               elseif {List.member {Label Request} [write delete]} then
                  proc {Trans TM}
                     {TM {Record.adjoinAt Request r _}}
                     {TM commit}
                  end
                  Client Stream
               in
                  Client = {Port.new Stream}
                  {Pbeer runTransaction(Trans Client paxos)}
                  case Stream.1
                  of abort(ErrorCode) then
                     Result = ErrorCode
                  else
                     Result = Stream.1
                  end
                  {System.show Result}
               elseif {Label Request} == read then
                  proc {Trans TM}
                     {TM {Record.adjoinAt Request v Result}}
                     {TM commit}
                  end
                  Client
               in
                  {System.show 'going to perform a read'}
                  Client = {Port.new _}
                  {Pbeer runTransaction(Trans Client paxos)}
               else
                  {Pbeer {Record.adjoinAt Request r Result}}
               end
               {System.show 'waiting for Result'}
               {Wait Result}
               {System.show 'got it'}
               {self toSocket({Value.toVirtualString Result 100 100})}
            end
            {self readLoop}
         catch E then
            {Blabla "Got exception: "#{Value.toVirtualString E 100 100}}
            {Blabla "the socket is closed. Client is gone"}
         end
      end

      meth randomReply(Kind)
         Choice
      in
         Choice = 1 + {OS.rand} mod 2
         {Blabla "Got a correct "#Kind#" message"}
         case Kind
         of put then
            {Blabla "going to reply "#{GetVS PUT Choice}} 
            {self toSocket({GetVS PUT Choice})}
         [] get then
            {Blabla "going to reply "#{GetVS GET Choice}}
            {self toSocket({GetVS GET Choice})}
         end
      end

      meth toSocket(VS)
         try
            %{self write(vs:{Value.toVirtualString VS 100 100}#"\n")}
            {self write(vs:VS#"\n")}
         catch _ then
            {Blabla "Apparently the client run away!"}
            {self close}
         end
      end
   end
   
   %% Loop for reading every input to the socket
   proc {Accept}
      Obj
   in 
      {Server accept(acceptClass:Accepted host:_ port:_ accepted:?Obj)}
      thread
         {Obj readLoop}  
      end 
      {Accept}
   end 
in
   %% Defining the arguments
   Args = try
             {Application.getArgs
              record(help(single char:[&? &h] default:false)
                     port(single char:[&p] type:int default:DEFAULTPORT)
                     ring(single char:&r type:atom default:RING_NAME)
                     store(single char:&s type:atom default:STORE_TKET)
                    )}
          catch _ then
             {Say 'Unrecognised arguments'}
             optRec(help:true)
          end
   %% Help message
   if Args.help orelse Args.1 \= nil then
      {Say "Usage: "#{Property.get 'application.url'}#" [option]"}
      {Say "Options:"}
      {Say '#'("  -p, --port NUM\tPort number for the socket (default "
               DEFAULTPORT ")")}
      {Say "  -r, --ring\tRing name (default: "#RING_NAME#")"}
      {Say "  -s, --store\tTicket to the store (default: "#STORE_TKET#")"}
      {Say "  -h, -?, --help\tThis help"}
      {Application.exit 0}
   end

   %% Defining verbose feedback
   %if Args.verbose then
      Blabla   = Say
      Bla      = System.printInfo
   %else
   %   Blabla   = proc {$ _} skip end
   %   Bla      = proc {$ _} skip end
   %end
  
   %% This part has to change to call the real pbeer
   Pbeer = {PbeerCommon.getPbeer Args.store Args.ring}
   local
      R
   in
      R = {Pbeer lookup(key:lucifer res:$)}
      {Wait R}
      {System.show R}
   end

   %% Let there be a socket connection
   try P in
      Server={New Open.socket init}
      {Server bind(port:P takePort:Args.port)}
      {Server listen}
      {Say "The socket server is listening to port number:"}
      {System.show P}
      {Accept}
   catch E then
      {Say "No socket connection."}
      {Say "Probably, the address is already in use. Here is the exception:"}
      {System.show E}
      {Say "----------------------------------------------------------------"}
      {Application.exit 1}
   end
end   
