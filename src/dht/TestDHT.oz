%% Test the DHT functionality implemented on DHT.oz

functor
import
   Property
   System
   Network        at '../network/Network.ozf'
   PbeerMaker     at '../pbeer/Pbeer.ozf'
   TestPairs      at 'TestPairs.ozf'
   TestSets       at 'TestSets.ozf'
export
   Run
define

   SIZE  = 42

   ComLayer
   MasterOfPuppets
   MasterId
   MaxKey
   Pbeers
   NetRef

   %% For feedback
   Say    = System.showInfo

   proc {CreateNetwork}
      %{System.printInfo 'first line'}
      {Say "Creating Master peer..."}
      MasterOfPuppets = {PbeerMaker.new args}
      {Say "Master peer created..."}
      %{System.show 'second line'}
      MasterId = {MasterOfPuppets getId($)}
      Pbeers = {List.make SIZE-1}
      NetRef = {MasterOfPuppets getFullRef($)}
      for Pbeer in Pbeers do
         Pbeer = {PbeerMaker.new args}
         {Pbeer join(NetRef)}
         thread
            Id
            proc {ReceivingLoop}
               NewMsg
            in
               {Pbeer receive(NewMsg)}
               {Wait NewMsg}
               {System.show 'Pbeer '#Id#' got '#NewMsg.text#' from '#NewMsg.src}
               {ReceivingLoop}
            end
         in
            Id = {Pbeer getId($)}
            {ReceivingLoop}
         end
         %{Delay 100}
      end
      ComLayer = {Network.new}
      {Delay 1000}
      local
         P I S
      in
         {MasterOfPuppets getPred(P)}
         {MasterOfPuppets getId(I)}
         {MasterOfPuppets getSucc(S)}
         {System.showInfo "MASTER: "#P.id#"<-"#I#"->"#S.id}
      end
   end

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" <test> [option]"}
      {Say ""}
      {Say "Tests:"}
      {Say "\tall\tRun all tests (default)"}
      {Say "\tpairs\tTest key/value pairs"}
      {Say "\tsets\tTest key/value-sets"}
      {Say ""}
      {Say "Options:"}
      {Say "  -h, -?, --help\tThis help"}
   end

   proc {Bootstrap}
      {CreateNetwork}
      {Say "Network bootstraped"}
      MaxKey = {MasterOfPuppets getMaxKey($)}
   end

   fun {TestAll}
      {Bool.and {TestPairs.test MasterOfPuppets}
                {TestSets.test MasterOfPuppets}}
   end
   
   fun {Run Args}

      {Property.put 'print.width' 1000}
      {Property.put 'print.depth' 1000}

      %% Help message
      if Args.help then
         {HelpMessage}
         false
      else 
         case Args.1
         of "dht"|Subcommand|nil then
            case Subcommand
            of "all" then
               {Bootstrap}
               {TestAll}
            [] "pairs" then
               {Bootstrap}
               {TestPairs.test MasterOfPuppets}
            [] "sets" then
               {Bootstrap}
               {TestSets.test MasterOfPuppets}
            else
               {Say "ERROR: Invalid invocation\n"}
               {Say {Value.toVirtualString Args 100 100}}
               {HelpMessage}
               false
            end
         [] nil then
            {Say "Running all threads"}
            {Bootstrap}
            {TestAll}
         else
            {Say "ERROR: Invalid invocation\n"}
            {Say {Value.toVirtualString Args 100 100}}
            {HelpMessage}
            false
         end
      end
   end

end
