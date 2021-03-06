% Testing the Key/Value Pairs operations: put/get/delete

functor
import
   System
   Constants      at '../commons/Constants.ozf'
   Utils          at '../utils/Misc.ozf'
   SimpleSDB      at 'SimpleSDB.ozf'
export
   Test
define

   Say      = System.showInfo
   Wisper   = System.printInfo

   NoValue  = Constants.noValue
   Success  = Constants.success
   BadSecret= Constants.badSecret

   MaxKey % Bound after knowning MasterOfPuppets 

   %% --------------------------------------------------------------------
   %% Definition of single test

   fun {PutAndGet Pbeer}
      R1 R2 K V S
   in
      K = {Name.new}
      V = {Name.new}
      S = {Name.new}
      {Wisper "put and get: "} 
      {Pbeer put(s:S k:K v:V r:R1)}
      if R1 == Success then
         {Pbeer get(k:K v:R2)}
         if R2 == V then
            {Say "PASSED"}
            true
         else
            {Say "FAILED - could not retrieve stored value"}
            false
         end
      else
         {Say "FAILED - Single put did not work"}
         false
      end
   end
   
   fun {GetNoValue Pbeer}
      {Wisper "get no value: "}
      if {Pbeer get(k:{Name.new} v:$)} == NoValue then
         {Say "PASSED"}
         true
      else
         {Say "FAILED: Creation out of nothing"}
         false
      end
   end

   fun {Delete Pbeer}
      R1 R2 K V S
   in
      K = {Name.new}
      V = {Name.new}
      S = {Name.new}
      {Wisper "delete : "}
      {Pbeer delete(k:{Name.new} s:{Name.new} r:R1)}
      if R1 == NoValue then
         {Pbeer put(k:K v:V s:S r:Success)}
         if {Pbeer get(k:K v:$)} == V then
            {Pbeer delete(k:K s:S r:R2)}
            if R2 == Success andthen {Pbeer get(k:K v:$)} == NoValue then
               {Say "PASSED"}
               true
            else
               {Say "FAILED: deleting existing item did not work"}
               false
            end
         else
            {Say "FAILED: putting did not work.... VERY STRANGE"}
            false
         end   
      else
         {Say "FAILED: Deleting unexisting element did not work"}
         false
      end
   end

   fun {WrongKeysOnPut Pbeer}
      R K V S
   in
      K = {Name.new}
      V = {Name.new}
      S = {Name.new}
      {Wisper "wrong keys on put : "}
      {Pbeer put(k:K v:{Name.new} s:S r:Success)}
      {Pbeer put(k:K v:V s:S r:Success)}
      if {Pbeer get(k:K v:$)} == V then
         %% testing wrong secret
         {Pbeer put(k:K v:{Name.new} s:{Name.new} r:R)}
         if R == BadSecret then
            %% testing wrong key
            {Pbeer put(k:{Name.new} v:{Name.new} s:S r:Success)}
            if {Pbeer get(k:K v:$)} == V then
               %% testing wrong K1 and Secret 
               {Pbeer put(k:{Name.new} v:{Name.new} s:{NewName} r:Success)}
               if {Pbeer get(k:K v:$)} == V then
                  {Say "PASSED"}
                  true
               else
                  {Say "FAILED: on wrong K and Secret"}
                  false
               end
            else
               {Say "FAILED: on wrong K1"}
               false
            end
         else
            {Say "FAILED: on wrong secret"}
         end
      else
         {Say "FAILED: on basic put. VERY STRANGE!"}
         false
      end
   end

   fun {WrongKeysOnGet Pbeer}
      K V S
   in
      K = {Name.new}
      V = {Name.new}
      S = {Name.new}
      {Wisper "wrong keys on get : "}
      {Pbeer put(k:K v:V s:S r:Success)}
      if {Pbeer get(k:K v:$)} == V then
         if {Pbeer get(k:{Name.new} v:$)} == NoValue then
            {Say "PASSED"}
            true
         else
            {Say "FAILED: on wrong K"}
            false
         end
      else
         {Say "FAILED: on basic put/get. VERY STRANGE!"}
      end
   end

   fun {WrongKeysOnDelete Pbeer}
      K V S
   in
      K = {Name.new}
      V = {Name.new}
      S = {Name.new}
      {Wisper "wrong keys on delete : "}
      {Pbeer put(k:K v:V s:S r:Success)}
      %% testing wrong K
      {Pbeer delete(k:{Name.new} s:S r:NoValue)}
      if {Pbeer get(k:K v:$)} == V then
         %% testing worng secret
         {Pbeer delete(k:K s:{Name.new} r:BadSecret)}
         if {Pbeer get(k:K v:$)} == V then
            {Say "PASSED"}
            true
         else
            {Say "FAILED: deleted item event with the wrong secret"}
            false
         end
      else
         {Say "FAILED: deleted value only with K2 and S but wrong K1"}
         false
      end
   end

   %% -------------------------------------------------------------------
   %% End of individual tests - going to global organization of tests 
   %% -------------------------------------------------------------------

   fun {Test MasterOfPuppets}
      Results = {NewCell nil}
      proc {AddTest ATest}
         Results := {ATest MasterOfPuppets}|@Results
      end
   in
      MaxKey = {MasterOfPuppets getMaxKey($)}
      {AddTest PutAndGet} 
      {AddTest GetNoValue} 
      {AddTest Delete}
      {AddTest WrongKeysOnPut}
      {AddTest WrongKeysOnGet}
      {AddTest WrongKeysOnDelete}
      {MasterOfPuppets send(msg(text:'hello nurse' src:foo)
                            to:{Utils.hash foo MaxKey})}
      {MasterOfPuppets send(msg(text:bla src:foo) 
                            to:{Utils.hash ina MaxKey})}
      {System.show 'TESTING DIRECT ACCESS TO THE STORE OF A PEER'}
      local
         Pbeer
      in
         {MasterOfPuppets lookup(key:foo res:Pbeer)}
         {MasterOfPuppets send(put(foo bla) to:Pbeer.id)}
      end
      {Delay 1000}
      local
         Pbeer HKey
      in
         HKey = {Utils.hash foo MaxKey} 
         {MasterOfPuppets lookupHash(hkey:HKey res:Pbeer)}
         {MasterOfPuppets send(putItem(HKey foo tetete tag:dht) to:Pbeer.id)}
      end
      {List.foldL @Results Bool.and true}
   end
end
