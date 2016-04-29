/*-------------------------------------------------------------------------
 *
 * TextFile.oz
 *
 *    Provides an interfice to read and write text files
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: boriss $
 *
 *    $Date: 2011-05-19 21:45:21 +0200 (Thu, 19 May 2011) $
 *
 * NOTES
 *      
 *    This functor is an interface to the classes from the Mozart Open module
 *    to create text files for read and write. The idea is to concentrate
 *    class inheritance and try catch operation on this file to avoid poluting
 *    the rest of the code. This functor is used as a library instead of a
 *    component, so no events are used.
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Compiler
   Open
   System
export
   New
   Read
   ReadLog
   TextFile
define

   %% Mixes classes Open.text and Open.file
   class TextFile from Open.text Open.file end

   fun {New Args}
      TheFile
      proc {Obj Msg}
         case Msg
         of write(Text) then
            {TheFile putS({Value.toVirtualString Text 1000 1000})}
         [] close then
            try
               {TheFile close}
            catch E then
               {System.show 'Error: Problem closing file '#Args.name}
               {System.show 'Exception: '#E}
            end
         else
            {TheFile Msg}
         end
      end
   in
      try
         TheFile = {Object.new TextFile Args}
      catch E then
         {System.show 'Error: Trying to open file '#Args.name}
         {System.show 'Exception: '#E}
         try
            {TheFile close}
         catch _ then
            skip
         end
      end
      %% Return the file object
      Obj
   end

   %% Read a file and return a stream of data depending on Tranform
   fun {ReadLoop FileName Transform Verbose}
      FN
   in
      try
         FN = {Object.new TextFile init(name:FileName)}
         fun {Loop}
            if {FN atEnd($)} then
               thread
                  try
                     {FN close}
                  catch _ then
                     skip
                  end
               end
               nil
            else
               Str = {FN getS($)}
            in
               try
                  Data = {Transform Str normal}
               in
                  Data|{Loop}
               catch _ then 
                  try
                     Data = {Transform Str fix}
                  in
                     Data|{Loop}
                  catch _ then
                     if Verbose then
                        {System.showInfo "Ignored line "#Str}
                     end
                     {Loop}
                  end
               end
            end
         end
      in
         {Loop}
      catch _ then
         thread
            try
               {FN close}
            catch _ then
               skip
            end
         end
         nil
      end
   end

   %% Read a file and return a stream of lines
   fun {Read FileName}
      fun {Transform Str Op}
         case Op
         of normal then
            {VirtualString.toString Str}
         [] fix then
            {VirtualString.toString Str#""}
         else
            Str
         end
      end
   in
      {ReadLoop FileName Transform true}
   end

   fun {ReadLog FileName}
      fun {Transform Str Op}
         case Op
         of normal then
            {Compiler.evalExpression Str env _}
         [] fix then
            {Compiler.evalExpression Str#"\n" env _}
         else
            Str
         end
      end
   in
      {ReadLoop FileName Transform false}
   end

end
