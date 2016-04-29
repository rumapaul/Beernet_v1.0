/*-------------------------------------------------------------------------
 *
 * Random.oz
 *
 *    Util functions to generate random numbers
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *            Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 405 $ $Author: ruma $
 *
 *    $Date: 2013-06-26 11:48:11 +0200 (Wed, 26 June 2013) $
 *
 *-------------------------------------------------------------------------
 */

functor
import
   OS
export
   SetSeed
   Urand
   UrandNoBounds
   UrandInt
define

   local
      RMin
      RMax
      {OS.randLimits RMin RMax}
   in

      %% Set seed using process id
      proc {SetSeed}
         %{OS.srand {OS.getPID}}
         skip
      end

      %% Returns a uniform random number [0,1]
      fun {Urand}
          {Int.toFloat {OS.rand} - RMin} / {Int.toFloat RMax - RMin}
      end
      %% Returns a uniform random number (0,1)
      fun {UrandNoBounds}
          {Int.toFloat {OS.rand} - RMin + 1} / {Int.toFloat RMax - RMin + 2}
      end
      %% Returns a uniform random integer number [From To]
      fun {UrandInt From To}
         From + {Float.toInt {Urand} * {Int.toFloat To - 1}}
      end
   end
end
