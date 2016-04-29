%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor
import
   Application
   Property
   System
   TestDHT        at 'TestDHT.ozf'
define

   %% For feedback
   %Show   = System.show
   Say    = System.showInfo
   Args
   
   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" <test> [option]"}
      {Say ""}
      {Say "Tests:"}
      {Say "\tdht\tTest the DHT running on a network"}
      {Say ""}
      {Say "Options:"}
      {Say "  -h, -?, --help\tThis help"}
   end

   proc {FinalMsg Flag}
      if Flag then
         {Say "\nPASSED"}
      else
         {Say "\nFAILED: Some tests did not pass. Check above for details"}
      end
   end

in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   %% Defining input arguments
   Args = try
             {Application.getArgs
              record(
                     help(single char:[&? &h] default:false)
                     )}

          catch _ then
             {Say 'Unrecognised arguments'}
             optRec(help:true)
          end

   %% Help message
   if Args.help then
      {HelpMessage}
      {Application.exit 0}
   end
   
   case Args.1
   of Command|_ then
      case Command
      of "dht" then
         {FinalMsg {TestDHT.run Args}}
      else
         {Say "ERROR: Invalid invocation\n"}
         {Say {Value.toVirtualString Args 100 100}}
         {HelpMessage}
      end
   else
      {Say "ERROR: Invalid invocation\n"}
      {Say {Value.toVirtualString Args 100 100}}
      {HelpMessage}
   end

   {Application.exit 0}
end
