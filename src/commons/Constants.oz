/*-------------------------------------------------------------------------
 *
 * Constants.oz
 *
 *    Static definition of default constants values and system parameters
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
 *    Last change: $Revision: 240 $ $Author: ruma $
 *
 *    $Date: 2016-04-07 17:23:21 +0200 (Mon, 7 Apr 2016) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   Abort
   BadSecret
   ErrorBadSec
   StaleSnapshot
   LargeKey
   NoAck
   NoSecret
   NotFound
   NoValue
   Public 
   Success
   SlSize

   MergerFanout
   MergerMLookupsPerPeriod
   MergerGamma

   PSPeriod
   PhasePeriod

   RlxRingJoinWait
   RlxRingAttemptToJoin

   IsVisual
   StatsFlag

   MsgTimeout
   MsgTries

   FDMonitoringLimit
   FDHistorySize
   FDAvgCoefficient
   FDStdCoefficient
   FDInitTimeout
   FDMinTimeout

   PbeerReplicationFactor
   SymmetricReplicationTimeout

   FingerTableK
   FTStatusValidityPeriod
   FTValidityPeriod
   FTValidityFactor

   PredCandidateValidityFactor
   PredConnValidityFactor

   PaxosVotingPeriod
   PaxosLeaderElectionPeriod
   LeaderFreshnessPeriod

   EagerPaxosVotingPeriod
   EagerPaxosLockPeriod

define

   Abort       = 'ABORT'      % 
   BadSecret   = bad_secret   % Incorrect secret
   ErrorBadSec = error(bad_secret) % Error: Incorrect secret
   StaleSnapshot = stale_snapshot   % Snapshot of the Data Item is being modified by some other transaction 	
   NoAck       = nack         % Used when no remote answer is needed
   NotFound    = 'NOT_FOUND'  % To be used inside the component as constant
   Public      = public       % No secret
   Success     = 'SUCCESS'    % Correct secret, or new item created

   %% Numbers
   LargeKey    = 2097152      % 2^21 used for max key
   SlSize      = 7            % successor list size (because I like 7)

   %% aliases
   NoValue  = NotFound
   NoSecret = Public

   %%ReCircle Parameters
   MergerFanout = 1           	% Acceptable Values 1-5
   MergerMLookupsPerPeriod = 2 	% Acceptable Values 1-5 and 100, 100 implies infinity
   MergerGamma = 5000		% Period to trigger merger (in ms)

   PSPeriod = 3000     		% Period of Periodic Stabilizer (in ms)
   PhasePeriod = 5000	        % Period of Periodic Phase Computation for setPhaseNotify

   RlxRingJoinWait = 5000       % Milliseconds to wait to retry a join 
   RlxRingAttemptToJoin = 30   % Number of tries to join before notifying the client

   IsVisual = 0               	% Flag to turn on visual debug messages

   StatsFlag = 1		% Flag to generate Statistics for experiments

   MsgTries = 5       		% Number of tries to send a message

   %% FD Parameters
   FDMonitoringLimit = 200 	% Max number of nodes to be monitored by a single node
   FDHistorySize = 50   	% History Size of RTT for a connection
   FDAvgCoefficient = 1 	% Co-efficient of Average RTT to calculate FD timeout
   FDStdCoefficient = 1 	% Co-efficient of STD of RTT to calculate FD timeout

   %% Data Layer Parameters
   PbeerReplicationFactor = 4 	% Replication Factor of each key

   %% RlxRing Parameters
   FingerTableK = 4   		% Factor K for K-ary fingers
   FTStatusValidityPeriod = 30000 % Period during which FT Status is valid, need to recalculate beyond this period 
   FTValidityPeriod = 60000     % Validity of current FT, need to be refreshed beyond this period
   FTValidityFactor = 10.0       % Max Difference between two subsequent FT Health calculation,  upto which FT will be considered as converged

   PredCandidateValidityFactor = 5      % Max Number of PS period beyond which an unacknowledged member of PredList will be considered invalid
   PredConnValidityFactor = 3    % Max Number of PS period beyond which no connection will be assumed with an unresponsive Pred (no RetirevePred is received during this period) 

   %% Configuration related RTT 
   MsgTimeout = 5000 		% Timeout (in ms) to retry sending a msg

   FDInitTimeout = 7000 	% Init Timeout (in ms) for FD 
   FDMinTimeout = 10000  	% Min Timeout (in ms) for FD

   SymmetricReplicationTimeout = 60000 % Timeout of Replica Layer to read all/majotiry of replicas of a key 

   PaxosVotingPeriod = 90000    % Timeout to collect majority for a transaction
   PaxosLeaderElectionPeriod = 60000 % Timeout to select a new leader for transaction
   LeaderFreshnessPeriod = 5000 % Timeout to start new leader election after suspicion of current leader

   EagerPaxosVotingPeriod = 90000    % Timeout to collect majority for a transaction
   EagerPaxosLockPeriod   = 180000   % Timeout to commit a transaction, after this period the lock will be released 

end

