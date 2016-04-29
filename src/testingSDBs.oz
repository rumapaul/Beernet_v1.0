{Property.put 'print.width' 1000}
{Property.put 'print.depth' 1000}

declare
[SDB] = {Module.link ['database/SimpleDB.ozf']}
SDB1 = {SDB.new}
SDB2 = {SDB.new}

local
   Res1 Res2
in
   {SDB1 dumpRange('from':2 to:10 res:Res1)}
   {SDB2 dumpRange('from':2 to:10 res:Res2)}
   {Wait Res1}
   {Wait Res2}
   {System.show Res1}
   {System.show Res2}
end

for I in 1..13;3 do
   {SDB1 put(I I*2 I*I)}
   if I mod 2 == 0 then
      {SDB1 put(I I*3 I*I)}
   end
end

local
   Res1
   Res2
in
   {SDB1 dumpRange('from':3 to:10 res:Res1)}
   {SDB2 insert(Res1)}
   {SDB2 dumpRange('from':3 to:10 res:Res2)}
   {System.show Res1==Res2}
   {System.show Res1}
   {System.show Res2}   
end

declare
[SDB] = {Module.link ['database/SimpleSDB.ozf']}
SDB1 = {SDB.new}
SDB2 = {SDB.new}

local
   Res1 Res2
in
   {SDB1 dumpRange(2 10 Res1)}
   {SDB2 dumpRange(2 10 Res2)}
   {Wait Res1}
   {Wait Res2}
   {System.show Res1}
   {System.show Res2}
end

for I in 1..13;3 do
   {SDB1 put(I I*2 I*I I _)}
   if I mod 2 == 0 then
      {SDB1 put(I I*3 I*I I _)}
   end
end

local
   Res1 R1
   Res2
in
   {SDB1 dumpRange(3 10 Res1)}
   {SDB2 insert(Res1 R1)}
   {SDB2 dumpRange(3 10 Res2)}
   {System.show Res1==Res2}
   {System.show Res1}
   {System.show Res2}   
   {System.show R1}
end

