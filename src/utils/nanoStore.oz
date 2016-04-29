%% 
%%                 --- nanoStore ---
%% A more reliable server that store the address of peers
%% running on the real world ;) (and that are less reliable)
%% 
%% Author: Boriss Mejias (boriss.mejias@uclouvain.be)
%%
%% Obs: After a while a realized that this program has a very similar
%%      goal as Kevin Glynn's OzStore. That's why I took the name of
%%      nanoStore, and I also reuse some of his code.
%%
functor
import
   Application
   Connection
   Gdbm at 'x-oz://contrib/gdbm'
   OS
   Pickle
   Property
   System
define
   VERSION          = 0.9
   %BEERNET_SERVER      = 'beernet.info.ucl.ac.be'
   %BEERNET_SERVER_PATH = '/var/www/p2ps/tickets/'
   TKET_FILENAME    = 'nano.tket'
   %TKET_TUETUE_PATH = {OS.getEnv 'HOME'}#'/public_html/php-mozart/tickets/'
   DB_NAME          = {OS.getEnv 'HOME'}#'/.nanoStore.db'
   
   %% We need the localhost information to decide where to place the ticket.
   %% This is because tests can be run on a local machine
   %% or on the p2psxen server.
   %LocalHost = {OS.uName}.nodename
   Say = System.showInfo %% For feedback to the standard output
   Args   %% Application arguments
   DictS  %% Stream of request
   DictP  %% Listening port
   DB     %% Gdbm database
   UniqueMarker = {Name.new}
   
   %% This object can be used in case you don't want (or cannot) use gdbm
   fun {MakeDictionaryDB}
      Dict
      proc {DBObject Msg}
         case Msg
         of close then
            skip
         [] put(Key Val) then
            Dict.Key := Val
         [] get(Key Val) then
            Entry = {Dictionary.condGet Dict Key UniqueMarker}
         in
            %% If client is in tempFail then the bind will block
            thread
               try
                  Val = if Entry \= UniqueMarker then
                           success(Entry)
                        else
                           error(keyNotInDictionary(Key))
                        end
               catch _ then
                  %% Don't care if the client has failed (permFail)
                  skip
               end
            end
         else
            {Say '#'("Message not understood\n"
                     "Got: " {Value.toVirtualString Msg 8 20})}
         end
      end
   in
      Dict = {Dictionary.new}
      DBObject
   end

   %% This object uses gdbm.
   fun {MakeGdbmDB}
      GdbmDB
      proc {GdbmObject Msg}
         case Msg
         of close then
            {Gdbm.close GdbmDB}
         [] put(Key Val) then
            {Gdbm.put GdbmDB Key Val}
         [] get(Key Val) then
            Entry = {Gdbm.condGet GdbmDB Key UniqueMarker}
         in
            %% If client is in tempFail then the bind will block
            thread
               try
                  Val = if Entry \= UniqueMarker then
                           success(Entry)
                        else
                           error(keyNotInDictionary(Key))
                        end
               catch _ then
                  %% Don't care if the client has failed (permFail)
                  skip
               end
            end   
         else
            {Say '#'("Message not understood\n"
                     "Got: " {Value.toVirtualString Msg 8 20})}
         end
      end 
   in
      GdbmDB = {Gdbm.new if Args.init then
                            new(Args.dbname mode:[owner])
                         else
                            create(Args.dbname mode:[owner])
                         end}
      GdbmObject
   end

   %% Behaviour of the nanoStore is written in this Loop
   %% I'm currently not doing anything with the peer-to-peer network
   %% Just storing and retrieving values from the persistent database.
   proc {Loop Msg}
      case Msg
      of shutdown then
         {Say "they told me to stop, so I will"}
         {DB close}
         {Application.exit 0}
      else
         {DB Msg}
      end
   end
in
   Args = try
             {Application.getArgs
              record(dbname(single char:[&d] type:atom default:DB_NAME)
                     init(single char:[&i] default:false)
                     help(single char:[&? &h] default:false)
                     persistent(single char:[&p] default:false)
                     ticket(single char:[&t] type:atom default:TKET_FILENAME)
                     version(single default:false)
                    )}
          catch _ then
             {Say 'Unrecognised arguments'}
             optRec(help:true)
          end
   %% Help message
   if Args.help then
      {Say "Usage: "#{Property.get 'application.url'}#" [option]"}
      {Say "Options:"}
      {Say "  -d, --dbname FILE\tDatabase name (default "#DB_NAME#")"} 
      {Say "  -i, --init BOOL\tCreate empty database (default false)"}
      {Say '#'("  -p, --persistent BOOL\tUses gdbm to store the db in a file "
               "(default false)")}
      {Say '#'("  -t, --ticket FILE\t"
               "Ticket to host the nanoStore (default "
               TKET_FILENAME#")")}
      {Say "      --version\t\tVersion number"}
      {Say "  -h, -?, --help\tThis help"}
      {Application.exit 0}
   end

   if Args.version then
      {Say "It is always cool to have a version number :)"}
      {Say "nanoStore "#VERSION}
      {Application.exit 0}
   end

   DB =  if Args.persistent then
            {MakeGdbmDB}
         else
            {MakeDictionaryDB}
         end
   
   %% Start listening to the Dictionary Stream
   thread
      for Msg in DictS do
         {Loop Msg}
      end
   end
   %% Create port for the Dictionary
   {NewPort DictS DictP}
   
   {Pickle.save {Connection.offerUnlimited DictP} Args.ticket}
   {Say "nanoStore started..."}
end   
   
